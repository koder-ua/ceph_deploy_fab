$fsid = '066F558C-6789-4A93-AAF1-5AF1BA01A3AD'

node /ceph-mon*/ {
  class { 'ceph::repo': }
  class { 'ceph':
    fsid                => $fsid,
    mon_host            => '192.168.152.42',
    mon_initial_members => 'ceph-mon.koder',
    authentication_type => 'none',
  }
  ceph::mon { $::hostname:
    authentication_type => 'none',
  }
}

node /ceph-osd*/ {
  class { 'ceph::repo': }
  class { 'ceph':
    fsid                => $fsid,
    mon_host            => '192.168.152.42',
    mon_initial_members => 'ceph-mon.koder',
    authentication_type => 'none',
  }
  ceph::osd {
  '/dev/sdb':
    journal => '/dev/sdb';
  }
}

node /client/ {
  class { 'ceph::repo': }
  class { 'ceph':
    fsid                => $fsid,
    mon_host            => '192.168.152.42',
    mon_initial_members => 'ceph-mon.koder',
    authentication_type => 'none',
  }
}