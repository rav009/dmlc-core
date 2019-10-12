#!/usr/bin/env python3
# pylint: disable=invalid-name
"""The container launcher script that launches DMLC with the right env variable."""
import glob
import sys
import os
import subprocess

def unzip_archives(ar_list, env):
    for fname in ar_list:
        if not os.path.exists(fname):
            continue
        if fname.endswith('.zip'):
            subprocess.call(args=['unzip', fname], env=env)
        elif fname.find('.tar') != -1:
            subprocess.call(args=['tar', '-xf', fname], env=env)

def main():
    """Main moduke of the launcher."""
    if len(sys.argv) < 2:
        print('Usage: launcher.py your command')
        sys.exit(0)

    hadoop_home = '/opt/cloudera/parcels/CDH/lib/hadoop'
    hdfs_home = '/opt/cloudera/parcels/CDH/lib/hadoop-hdfs'
    java_home = os.getenv('JAVA_HOME')
    cluster = 'yarn'

    assert cluster is not None, 'need to have DMLC_JOB_CLUSTER'

    env = os.environ.copy()
    library_path = ['./']
    class_path = []

    assert hadoop_home is not None, 'need to set HADOOP_HOME'
    assert hdfs_home is not None, 'need to set HADOOP_HDFS_HOME'
    assert java_home is not None, 'need to set JAVA_HOME'

    library_path.append('%s/lib/native' % hdfs_home)
    library_path.append('%s/lib' % hdfs_home)
    class_path = '/etc/hadoop/conf:/opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24/lib/hadoop/libexec/../../hadoop/lib/*:/opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24/lib/hadoop/libexec/../../hadoop/.//*:/opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24/lib/hadoop/libexec/../../hadoop-hdfs/./:/opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24/lib/hadoop/libexec/../../hadoop-hdfs/lib/*:/opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24/lib/hadoop/libexec/../../hadoop-hdfs/.//*:/opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24/lib/hadoop/libexec/../../hadoop-yarn/lib/*:/opt/cloudera/parcels/CDH-5.14.0-1.cdh5.14.0.p0.24/lib/hadoop/libexec/../../hadoop-yarn/.//*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/lib/*:/opt/cloudera/parcels/CDH/lib/hadoop-mapreduce/.//*'
    for f in str(classpath).split(':'):
        class_path += glob.glob(f)

    if java_home:
        library_path.append('%s/jre/lib/amd64/server' % java_home)

    env['CLASSPATH'] = '${CLASSPATH}:' + (':'.join(class_path))

    # setup hdfs options
    if 'DMLC_HDFS_OPTS' in env:
        env['LIBHDFS_OPTS'] = env['DMLC_HDFS_OPTS']
    elif 'LIBHDFS_OPTS' not in env:
        env['LIBHDFS_OPTS'] = '--Xmx128m'

    LD_LIBRARY_PATH = env['LD_LIBRARY_PATH'] if 'LD_LIBRARY_PATH' in env else ''
    
    # add the libhdfs path in the end of the row
    env['LD_LIBRARY_PATH'] = LD_LIBRARY_PATH + ':' + ':'.join(library_path) + ':' + '/opt/libhdfs'

    # unzip the archives.
    if 'DMLC_JOB_ARCHIVES' in env:
        unzip_archives(env['DMLC_JOB_ARCHIVES'].split(':'), env)

    ret = subprocess.call(args=sys.argv[1:], env=env)
    sys.exit(ret)


if __name__ == '__main__':
    main()
