from collections import namedtuple

from pycds import Network

TestNetwork = namedtuple("TestNetwork", "name long_name color publish")


def mknet(*args):
    return Network(**TestNetwork(*args)._asdict())


def get_networks():
    networks = [
        mknet(
            "EC",
            "Environment Canada (Canadian Daily Climate Data 2007)",
            "#FF0000",
            True,
        ),
        mknet("BCH", "BC Hydro", "#0010A5", True),
        mknet(
            "ARDA", "Agricultural and Rural Development Act Network", "#5791D9", True
        ),
        mknet(
            "EC_raw",
            'Environment Canada (raw observations from "Clima...)',
            "#FF0000",
            True,
        ),
        mknet("FLNRO-WMB", "BC Ministry of Forests, Lands, and ...", "#0C6600", True),
        mknet("AGRI", "BC Ministry of Agriculture", "#801899", True),
        mknet(
            "ENV-AQN",
            "BC Ministry of Environment - Air Quality Network",
            "#B03060",
            True,
        ),
        mknet(
            "MoTIe",
            "Ministry of Transportation and Infrastructure (ele...",
            "#37ea00",
            True,
        ),
    ]
    return {net.name: net for net in networks}
