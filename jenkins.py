import subprocess, os, sys, psutil, time

current_dir = sys.argv[1]
skip = ['.git', '.gitignore', 'README.md', 'INSTALL.md', 'license.txt', 'dtest.py', 'loadmaker.py']
for test in os.listdir(current_dir):
    if test in skip:
        continue
    args = ['nosetests', '--nocapture', '--verbosity=3', '--with-xunit', '--xunit-file=%s.xml' % test, test]
    p = subprocess.Popen(args)
    process = psutil.Process(p.pid)
    gone, alive = psutil.wait_procs([process], 800)
    for p in alive:
        p.kill()
        jps = subprocess.Popen(['jps'], stdout=subprocess.PIPE)
        out, err = jps.communicate()
        lines = out.split('\n')
        for line in lines:
            if line.find('Cassandra') > 0:
                pid = line.split(' ')[0]
                cass = psutil.Process(int(pid))
                cass.kill()
