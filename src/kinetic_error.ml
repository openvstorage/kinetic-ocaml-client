(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
 *)


module Error = struct
  type msg = string

  type t =
    | KineticError of int * msg
    | Generic of string * int * msg
    | Timeout of float * msg

  let show = function
    | KineticError (rc,b) -> Printf.sprintf "KineticError(%i, %S)" rc b
    | Generic (file,line, b) -> Printf.sprintf "Generic(%s,%i %S)" file line b
    | Timeout (d,msg) -> Printf.sprintf "Timeout(%f,%S)" d msg
end
