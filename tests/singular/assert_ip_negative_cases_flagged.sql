-- Every known-corrupted management IP must be classed INVALID (returns the rows that are not).
-- Ported from telco_v2_core tests/assert_ip_negative_cases_flagged.sql, extended with the
-- corrupted patterns observed in NETWORK_ELEMENT (ports, URLs, SQL, full-width digits, hex octets).
select element_id, ip_address, ip_class
from {{ ref('dq_ip_address_validation') }}
where ip_address in ('192.168.1', '10.0.0.', '256.1.1.1', '10.0.0.1.5', '192.168.1.-1', '1O.0.0.1', '10,0,0,1',
                     '192.168.1.1/33', '2001:db8::g1', '2001:db8:::1', '1:2:3:4:5:6:7:8:9', 'localhost', '10..0.1',
                     '#@!$%^&*()', ' 10.0.0.1 ', '10.1.0.-1', '10,1,0,1', '10 1 0 1', 'abc.def.ghi.jkl', '10.1.0.1/24',
                     '10.1.0.1:8080', 'http://10.1.0.1', '0x0A.0x01.0x00.0x01', '12345::abcd::1', '10.1.0.1#',
                     '10.1.0.@1', '10.1.0.1;DROP TABLE x', '10.1.0.1''', '"10.1.0.1"', '10.1.0.1%eth0', 'N/A')
  and ip_class <> 'INVALID'
