
------------------------------------------------
FROM THE CS343 STUDENT TEAM:

for checkpoint 1, our debugging code is in the file peer.go, and our skeleton for the peer-pressure
algorithm is in the file peer-pressure.go.
To run it follow the same logic as you would for peer.go (see below)

-------------------------------------------------
To run the code, do the following:
1- specify the peer information in cluster.txt
2- for each peer, run the code using the following command:

    go run peer.go <num> <location of config file>

For example, to start peer 0; first in cluster.txt that resides in the same directory as peer.go:
    go run peer.go 0 cluster.txt
Happy debugging!