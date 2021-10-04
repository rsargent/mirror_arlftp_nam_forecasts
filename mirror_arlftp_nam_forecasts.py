import datetime, ftplib, getpass, mgzip, io, os, sys

logfilename = os.path.splitext(os.path.realpath(__file__))[0] + '.log'

logfile = open(logfilename, 'a')

def log(msg):
    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    full_msg = f"{now} pid={os.getpid()} {msg}\n"
    logfile.write(full_msg)
    logfile.flush()
    sys.stdout.write(full_msg)
    sys.stdout.flush()

log(f'Starting {os.path.realpath(__file__)} with python {sys.executable} as username {getpass.getuser()}')

ftp_timeout_secs = 10*60 # 10 minutes
hostname = 'ftp.arl.noaa.gov'
ftp = ftplib.FTP(hostname, timeout=ftp_timeout_secs)
ftp.login()
log(f"Logged in to {hostname}")
# world-writable
os.umask(0)

today = datetime.date.today()

for days_ago in range(7, -1, -1):
    mirror_date = today - datetime.timedelta(days = days_ago)
    for hour in range(0, 24, 6):
        for future_hr in range(0, 48, 6):
            filename = f"{mirror_date.strftime('%Y%m%d')}/hysplit.t{hour:02d}z.namsf{future_hr:02d}.CONUS"
            src = f"/forecast/{filename}"
            dest = f"/projects/earthtime/air-data/nam-forecast/{filename}.gz"
            if not os.path.exists(dest):
                log(f'Mirroring {src} ... ')
                bytesio = io.BytesIO()
                dir = os.path.dirname(src)
                try:
                    ftp.cwd(dir)
                except ftplib.error_perm as e:
                    log(f'could not change to directory {dir}, skipping')
                    continue
                try:
                    ftp.retrbinary(f'RETR {os.path.basename(src)}', lambda data: bytesio.write(data))
                except ftplib.error_perm as e:
                    log(f'could not download file {os.path.basename(src)}, skipping')
                    continue
                bytes = bytesio.getvalue()
                log(f"received {len(bytes)/1e6:.6f} MB")
                compressio = io.BytesIO()
                mgzip.GzipFile(fileobj=compressio, mode='w', thread=12).write(bytes)
                compressed = compressio.getvalue()
                dest = f"/projects/earthtime/air-data/nam-forecast/{filename}.gz"
                os.makedirs(os.path.dirname(dest), exist_ok=True)
                dest_tmp = f"{dest}.tmp{os.getpid()}"
                open(dest_tmp, 'wb').write(compressed)
                os.rename(dest_tmp, dest)
                log(f"Wrote {dest} ({len(compressed)/1e6:.6f} MB)")

log(f'Completed.  Exiting with status 0')
sys.exit(0)

