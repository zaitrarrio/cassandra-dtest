import subprocess, os, sys, psutil, time

current_dir = sys.argv[1]
for test in os.listdir(current_dir):
    args = ['nosetests', '--nocapture', '--verbosity=3', '--with-xunit', '--xunit-file=%s.xml' % test]
    p = subprocess.Popen(args)
    process = psutil.Process(p.pid)
    gone, alive = psutil.wait_procs([process], 800)
    for p in alive:
        p.kill()
