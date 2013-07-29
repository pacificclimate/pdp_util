database:
  dsn: "[filled in programatically]"
  id: "vars_id"
  table: "(SELECT vars_id, network_name, net_var_name, standard_name, cell_method, unit FROM meta_vars NATURAL JOIN meta_network WHERE cell_method !~ '(within|over)') AS foo"

dataset:
  name: "Variable metadata"
  description: "Metadata describing all of the meteorological quantities for which PCIC has archived data"

sequence:
  name: "variables"

network:
  name: "network"
  col: "network_name"
  type: String

variable:
  name: "variable"
  col: "net_var_name"
  type: String

standard_name:
  name: "standard_name"
  col: "standard_name"
  type: String
  reference: "http://llnl.gov/"

cell_method:
  name: "cell_method"
  col: "cell_method"
  type: String
  reference: "http://llnl.gov/"

unit:
  name: unit
  col: unit
  type: String


#database:
#    dsn: "postgresql://httpd:R3@d0nl^@windy/crmp"
#    table: !Query 'SELECT network, net_var_name, standard_name, cell_method, unit FROM meta_vars NATURAL JOIN meta_network'
#
#dataset:
#    NC_GLOBAL:
#        made_by: james
#
#sequence:
#    name: network
#    col: network
#
#net_var_name:
#    name: var_name
#    col: net_var_name
#
#unit:
#    name: unit
#    col: unit

