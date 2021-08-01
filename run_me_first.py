# https://stackoverflow.com/a/66372128/14012992
import os, sys

src_path = os.path.join(os.path.dirname(__file__), 'src')
pth_path = os.path.join(os.path.dirname(os.__file__), 'site-packages', 'ht_data.pth')
with open(pth_path, 'w+') as f:
    f.write(src_path)
print(f'added {src_path} to {pth_path}')
print(sys.path)