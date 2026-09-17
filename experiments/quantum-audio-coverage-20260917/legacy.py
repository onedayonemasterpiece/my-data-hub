#!/usr/bin/env python3
"""Audit v1 against v1, never mistake an editorial difference between versions for a TTS omission."""
from __future__ import annotations
import argparse
import base64
import json
import runpy
import time
import zlib
from pathlib import Path
from audit import load, save, sha, pcm, decode, similarity, compare, transcribe, RATE

ROOT=Path('coverage-v1')
SCRIPT_SHA='ef5e1b65ab2d61849b24227b666a4148a59854e3106cf560d70913b1e643ff1a'

def audit(shard: int):
    from faster_whisper import WhisperModel
    import hashlib
    old=Path('experiments/lecture-tts-20260915')
    data=zlib.decompress(base64.b64decode(''.join((old/f'script.part{i}.b64').read_text().strip() for i in range(1,5))))
    assert hashlib.sha256(data).hexdigest()==SCRIPT_SHA
    payload=json.loads(data)
    expected=runpy.run_path(str(old/'longform_shard.py'))['chunk_slides'](payload)
    assert len(expected)==53
    metadata=load(ROOT/'final/final-manifest.json');mp3=ROOT/'final'/metadata['output']
    assert sha(mp3)==metadata['sha256']
    decoded=decode(mp3, ROOT/'full.wav')
    rows={}
    for path in sorted((ROOT/'raw').glob('shard-*.json')):
        receipt=load(path);assert receipt['script_sha256']==SCRIPT_SHA
        for r in receipt['chunks']:
            assert r['index'] not in rows
            rows[r['index']]=r
    assert sorted(rows)==list(range(1,54))
    model=WhisperModel('medium',device='cpu',compute_type='int8',cpu_threads=4,download_root='/tmp/whisper-coverage')
    cursor=0;report=[]
    for n in range(1,54):
        r=rows[n];p=ROOT/'raw/chunks'/r['wav'];assert sha(p)==r['wav_sha256']==metadata['chunk_sha256'][str(n)]]
        source=pcm(p);end=cursor+len(source)
        assert r['text']==expected[n-1]['text'] and r['slide']==expected[n-1]['slide']
        if (n-1)%6==shard:
            t=time.time();recognized=transcribe(model,p)
            item={'chunk':n,'slide':r['slide'],'expected_text':r['text'],'asr':recognized,'comparison':compare(r['text'],recognized['text']),'base_start_seconds':cursor/RATE,'duration_seconds':len(source)/RATE,'assembly':similarity(source,decoded[cursor:end]),'recognition_seconds':time.time()-t}
            report.append(item);save(ROOT/'report'/f'legacy-{shard}.json',{'script_sha256':SCRIPT_SHA,'recognizer':'faster-whisper medium CPU int8','items':report})
            print(json.dumps({'chunk':n,'comparison':item['comparison'],'recognized':recognized['text'],'assembly':item['assembly']},ensure_ascii=False),flush=True)
        cursor=end
    assert cursor==len(decoded),'First MP3 dropped or duplicated source samples'

def collect():
    rows=[]
    for p in sorted((ROOT/'reports').glob('legacy-*.json')):rows.extend(load(p)['items'])
    rows.sort(key=lambda r:r['chunk']);assert [r['chunk'] for r in rows]==list(range(1,54))
    save(ROOT/'report/full-v1-speech-audit.json',{'script_sha256':SCRIPT_SHA,'checked_chunks':53,'suspected_omission_chunks':[r['chunk'] for r in rows if r['comparison']['possible_omission']],'items':rows,'caution':'ASR differences require review; this is the original v1 script, not the expanded v2 script.'})

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('mode',choices=['audit','collect']);p.add_argument('--shard',type=int,default=0);a=p.parse_args()
    audit(a.shard) if a.mode=='audit' else collect()
