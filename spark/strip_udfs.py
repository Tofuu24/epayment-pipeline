import io
with io.open('c:\\Users\\Lenovo\\Documents\\epayment-pipeline\\spark\\settlement_monitor_job.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# find index of "def next_timeout_ms(s, watermark_ms):"
end_idx = 0
for i, line in enumerate(lines):
    if "def _ms(col):" in line:
        end_idx = i - 1
        break

start_idx = 0
for i, line in enumerate(lines):
    if "def grace_ms(rail):" in line:
        start_idx = i - 1
        break

with io.open('c:\\Users\\Lenovo\\Documents\\epayment-pipeline\\spark\\settlement_monitor_job.py', 'w', encoding='utf-8') as f:
    f.writelines(lines[:start_idx] + ['# UDFs moved to settlement_udfs.py\n'] + lines[end_idx:])
