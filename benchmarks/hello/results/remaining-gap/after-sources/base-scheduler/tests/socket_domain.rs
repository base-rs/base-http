// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use base_scheduler::{SocketAddress, SocketDomain};

#[test]
fn domains_follow_the_address_family() {
    for (address, domain) in [
        ("127.0.0.1:80", SocketDomain::Ipv4),
        ("[::1]:80", SocketDomain::Ipv6),
    ] {
        let address: SocketAddress = address.parse().unwrap();
        assert_eq!(SocketDomain::from(address), domain);
    }
}
