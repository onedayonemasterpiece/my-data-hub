# Kaggle input mount discovery

Kaggle may normalize or change the directory name used beneath `/kaggle/input`; provider slugs are therefore not filesystem authority. Ordinary master and embedding-worker bootstraps recursively discover their bounded input manifests from the fixed Kaggle input root.

Discovery is fail closed. Required files must be regular, non-symlink files beneath non-symlink ancestors, within per-file and inventory bounds, with an exact expected hash or task identity. All master runtime assets must resolve into one attached Dataset root. Status files must form one exact co-located set. Zero or multiple matches, a different required file set, unsafe paths, oversized inputs, or an input-pins mismatch aborts before credentials or database work.

The filesystem discovery only locates bytes. Authority remains the exact numeric private Dataset claims carried in execution pins and status metadata; no Kaggle credential is present in generated source.
