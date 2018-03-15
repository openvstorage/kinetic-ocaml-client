let to_hex = function
  | "" -> ""
  | s ->
     let n_chars = String.length s * 3 in
     let buf = Buffer.create n_chars in
     let hex c = Printf.sprintf "%02x " (Char.code c) in
     String.iter (fun c -> Buffer.add_string buf (hex c)) s;
     Buffer.sub buf 0 (n_chars - 1)
