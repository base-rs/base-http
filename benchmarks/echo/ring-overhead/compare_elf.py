#!/usr/bin/env python3
"""Verify loaded sections after documentation/dependency-minimum rebuilds."""
import argparse,hashlib,json,struct
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('measured',type=Path);p.add_argument('rebuilt',type=Path);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
def sections(path):
    b=path.read_bytes();assert b[:6]==b'\x7fELF\x02\x01'
    offset=struct.unpack_from('<Q',b,40)[0];size,count,index=struct.unpack_from('<HHH',b,58)
    headers=[struct.unpack_from('<IIQQQQIIQQ',b,offset+i*size) for i in range(count)]
    h=headers[index];names=b[h[4]:h[4]+h[5]]
    return {names[h[0]:names.index(b'\0',h[0])].decode():{'sha256':hashlib.sha256(b[h[4]:h[4]+h[5]]).hexdigest(),'bytes':h[5],'address':h[3],'flags':h[2],'file_offset':h[4]} for h in headers if h[2]&2}
left=sections(a.measured);right=sections(a.rebuilt)
differences=sorted(n for n in left.keys()|right.keys() if left.get(n)!=right.get(n))
assert set(differences)<={'.note.gnu.build-id'},differences
a.output.write_text(json.dumps({'measured':left,'rebuilt':right,'differences':differences,'runtime_sections_identical_except_build_id':True},indent=2)+'\n')
print('Runtime sections match; differences:',differences)
