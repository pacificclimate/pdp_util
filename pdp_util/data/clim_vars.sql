database:
  dsn: "[filled in programatically]"
  id: "vars_id"
  table: "(SELECT vars_id, network_name, net_var_name, standard_name, cell_method, unit FROM meta_vars NATURAL JOIN meta_network WHERE cell_method ~ '(within|over)') AS foo"

dataset:
  name: "Variable metadata"
  description: "Metadata describing all of the climatological averages for which PCIC has calculated climate normals"
  reference: "Faron???"

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
  reference: "http://cf-pcmdi.llnl.gov/documents/cf-standard-names/"

cell_method:
  name: "cell_method"
  col: "cell_method"
  type: String
  reference: "http://cf-pcmdi.llnl.gov/documents/cf-conventions/1.1/ch07s03.html"

unit:
  name: unit
  col: unit
  type: String
