"""Reference dump with fcsparser 0.2.8 (channel_naming='$PnN', raw values, no reformat) → CSV, plus meta."""
import sys, json, numpy as np, pandas as pd, fcsparser, warnings
warnings.simplefilter('ignore')
path, out = sys.argv[1], sys.argv[2]
meta, data = fcsparser.parse(path, reformat_meta=False, channel_naming='$PnN')
data = data.astype('float64')
# flowCore/operator semantics: truncate values above $PnR to $PnR (fcsparser does NOT do this)
if '--truncate' in sys.argv:
    for i, c in enumerate(data.columns, start=1):
        r = float(meta.get(f'$P{i}R', 'inf'))
        data[c] = data[c].clip(upper=r)
data.to_csv(out, index=False, float_format='%.17g')
print(json.dumps({'file': path, 'version': meta['__header__']['FCS format'].decode() if isinstance(meta['__header__']['FCS format'], bytes) else meta['__header__']['FCS format'],
                  'datatype': meta.get('$DATATYPE'), 'byteord': meta.get('$BYTEORD'), 'tot': int(meta['$TOT']), 'par': int(meta['$PAR']),
                  'channels': list(data.columns)}))
