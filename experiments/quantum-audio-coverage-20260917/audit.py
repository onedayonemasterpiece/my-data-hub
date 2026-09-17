#!/usr/bin/env python3
"""Independent, unprompted ASR plus sample-aligned comparison to source WAVs."""
from __future__ import annotations
import argparse
import difflib
import hashlib
import json
import re
import subprocess
import time
import wave
from pathlib import Path

RATE = 24000
SCRIPT_SHA = '21287df14140bfb7843ca05a42985a0bc9cfdf2c8876a15f7a7c37122fbcae14'

def sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()

def load(p: Path):
    return json.loads(p.read_text(encoding='utf-8'))

def save(p: Path, value):
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(value, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

def words(text: str) -> list[str]:
    from num2words import num2words
    text = text.lower().replace('ё', 'е')
    text = re.sub(r'\d+', lambda m: num2words(int(m.group()), lang='ru'), text)
    return re.findall(r'[a-zа-я]+', text)

def compare(expected: str, recognized: str) -> dict:
    a, b = words(expected), words(recognized)
    edits, matches = [], 0
    for tag, i, j, k, l in difflib.SequenceMatcher(None, a, b, autojunk=False).get_opcodes():
        if tag == 'equal':
            matches += j-i
        else:
            edits.append({'operation': tag, 'source_start_word': i, 'source_end_word': j, 'expected': ' '.join(a[i:j]), 'heard': ' '.join(b[k:l]), 'expected_count': j-i, 'heard_count': l-k, 'at_start': i == 0, 'at_end': j == len(a)})
    suspect = any(e['expected_count'] >= 3 and e['heard_count'] <= max(1,e['expected_count']//2) for e in edits)
    return {'expected_words': len(a), 'recognized_words': len(b), 'exact_word_recall': matches/max(1,len(a)), 'possible_omission': suspect, 'differences': edits}

def pcm(path: Path):
    import numpy as np
    with wave.open(str(path), 'rb') as w:
        assert (w.getframerate(),w.getsampwidth(),w.getnchannels()) == (RATE,2,1)
        return np.frombuffer(w.readframes(w.getnframes()), dtype='<i2').copy()

def decode(path: Path, target: Path):
    subprocess.run(['ffmpeg','-v','error','-y','-i',str(path),'-ac','1','-ar',str(RATE),'-c:a','pcm_s16le',str(target)], check=True, timeout=180)
    return pcm(target)

def similarity(a,b) -> dict:
    import numpy as np
    if len(a) != len(b):
        return {'same_sample_count':False,'source_samples':len(a),'target_samples':len(b)}
    x,y=a.astype('float64'),b.astype('float64')
    dot=float(np.dot(x,y)); xx=float(np.dot(x,x)); yy=float(np.dot(y,y))
    correlation=dot/max(1,(xx*yy)**0.5)
    error=float(np.dot(x-y,x-y))
    return {'same_sample_count':True,'correlation':round(correlation,8),'snr_db':round(10*np.log10(max(1,xx)/max(1,error)),3),'energy_ratio':round(yy/max(1,xx),6)}

def transcribe(model, path: Path) -> dict:
    # Deliberately no prompt, hotwords, target text or forced alignment.
    segments, info = model.transcribe(str(path),language='ru',beam_size=5,temperature=0.0,condition_on_previous_text=False,vad_filter=False,word_timestamps=True)
    segments=list(segments)
    return {'text':' '.join(s.text.strip() for s in segments),'segments':[{'start':s.start,'end':s.end,'text':s.text,'avg_logprob':s.avg_logprob,'words':[{'start':w.start,'end':w.end,'word':w.word,'probability':w.probability} for w in (s.words or [])]} for s in segments]}

def audit(root: Path, shard: int):
    from faster_whisper import WhisperModel
    reviewed, numbered = root/'reviewed',root/'numbered'
    plan=load(reviewed/'plan.json');receipt=load(reviewed/'chunk-source-receipt.json')
    assert sha(reviewed/'narration.json') == plan['script_sha256'] == SCRIPT_SHA
    assert [r['index'] for r in receipt['chunks']] == list(range(1,88))
    metadata=load(reviewed/'final-manifest.json'); cues_meta=load(numbered/'final-manifest.json')
    base_mp3=reviewed/metadata['mp3']; cue_mp3=numbered/cues_meta['mp3']
    assert sha(base_mp3) == metadata['mp3_sha256']
    assert sha(cue_mp3) == cues_meta['mp3_sha256']
    base=decode(base_mp3,root/'base-decoded.wav'); annotated=decode(cue_mp3,root/'numbered-decoded.wav')
    assert len(base)==round(metadata['pcm_duration_seconds']*RATE)
    assert len(annotated)==round(cues_meta['pcm_duration_seconds']*RATE)
    cue_chapters={c['heading']:c for c in load(numbered/'chapters.json')}
    rows={r['index']:r for r in receipt['chunks']}
    cursor=0;previous=None;timeline={}
    for r in receipt['chunks']:
        if previous is not None: cursor+=round(RATE*(0.9 if r['slide']!=previous else 0.18))
        frames=round(float(r['probe']['format']['duration'])*RATE)
        timeline[r['index']]=(cursor,cursor+frames)
        cursor+=frames;previous=r['slide']
    assert cursor==len(base),'Timeline does not cover the original audio'
    model=WhisperModel('medium',device='cpu',compute_type='int8',cpu_threads=4,download_root='/tmp/whisper-coverage')
    report=[]
    for n in range(1,88):
        if (n-1)%16!=shard: continue
        r=rows[n]; expected=plan['chunks'][n-1]
        assert r['text']==expected['text'] and r['slide']==expected['slide']
        raw=root/('replacement' if n==18 else 'raw')/'chunks'/r['wav']
        assert sha(raw)==r['wav_sha256']
        source=pcm(raw);start,end=timeline[n]
        assert len(source)==end-start
        chapter=cue_chapters[r['slide']]
        new_start=round(chapter['body_start_seconds']*RATE)+start-round(chapter['source_start_seconds']*RATE)
        new_end=new_start+len(source)
        base_segment=base[start:end]; numbered_segment=annotated[new_start:new_end]
        assembly={'raw_to_first_mp3':similarity(source,base_segment),'first_mp3_to_numbered_mp3':similarity(base_segment,numbered_segment),'first_second':similarity(source[:RATE],base_segment[:RATE]),'last_second':similarity(source[-RATE:],base_segment[-RATE:]),'numbered_first_second':similarity(base_segment[:RATE],numbered_segment[:RATE]),'numbered_last_second':similarity(base_segment[-RATE:],numbered_segment[-RATE:])}
        t=time.time();recognized=transcribe(model,raw)
        item={'chunk':n,'slide':r['slide'],'expected_text':r['text'],'raw_sha256':r['wav_sha256'],'base_start_seconds':start/RATE,'numbered_start_seconds':new_start/RATE,'duration_seconds':len(source)/RATE,'assembly':assembly,'asr':recognized,'comparison':compare(r['text'],recognized['text']),'recognition_seconds':time.time()-t}
        report.append(item)
        save(root/'report'/f'audit-{shard:02d}.json',{'recognizer':'faster-whisper medium CPU int8','script_sha256':SCRIPT_SHA,'no_reference_text_prompt':True,'items':report})
        print(json.dumps({'chunk':n,'slide':r['slide'],'comparison':item['comparison'],'recognized':recognized['text'],'assembly':assembly},ensure_ascii=False),flush=True)
    print(f'Completed unprompted speech audit: {len(report)} original WAVs',flush=True)

def collect(root: Path):
    rows=[]
    for p in sorted((root/'reports').glob('audit-*.json')): rows.extend(load(p)['items'])
    rows.sort(key=lambda r:r['chunk'])
    assert [r['chunk'] for r in rows]==list(range(1,88))
    report={'script_sha256':SCRIPT_SHA,'checked_chunks':len(rows),'checked_slides':len({r['slide'] for r in rows}),'suspected_omission_chunks':[r['chunk'] for r in rows if r['comparison']['possible_omission']],'items':rows,'caution':'ASR differences are candidates for review, not automatic proof of a speech omission.'}
    save(root/'report'/'full-speech-audit.json',report)
    print('ALL CHUNKS AUDITED; suspected omissions', report['suspected_omission_chunks'],flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['audit','collect']);p.add_argument('--root',type=Path,default=Path('coverage'));p.add_argument('--shard',type=int,default=0);a=p.parse_args()
    audit(a.root,a.shard) if a.mode=='audit' else collect(a.root)
