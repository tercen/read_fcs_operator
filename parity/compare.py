"""Compare fcsdump CSV vs fcsparser reference CSV: shape, channel names, max abs/rel diff."""
import sys, json, numpy as np, pandas as pd
rust, ref = pd.read_csv(sys.argv[1]), pd.read_csv(sys.argv[2])
name=sys.argv[3]
res={'file':name,'rust_shape':rust.shape,'ref_shape':ref.shape}
if rust.shape!=ref.shape: res['verdict']='SHAPE MISMATCH'; print(json.dumps(res)); sys.exit(1)
cols_equal=[c.replace(',','_') for c in ref.columns]==list(rust.columns); res['channels_equal']=cols_equal
a=rust.to_numpy(np.float64); b=ref.to_numpy(np.float64)
both_nan=(np.isnan(a)&np.isnan(b))|((a==b)&~np.isfinite(a)); a=np.where(both_nan,0,a); b=np.where(both_nan,0,b)  # NaN==NaN, ±inf==±inf
d=np.abs(a-b); rel=d/np.maximum(np.abs(b),1e-30)
res['max_abs']=float(d.max()) if d.size else 0.0; res['max_rel']=float(rel.max()) if d.size else 0.0; res['n_diff_gt_1e-9']=int((d>1e-9).sum())
res['nan_both']=int(both_nan.sum()); res['verdict']='EXACT' if res['max_abs']==0 else ('OK<1e-6' if res['max_rel']<1e-6 else 'DIFF')
print(json.dumps(res)); sys.exit(0 if res['verdict']!='DIFF' else 2)
