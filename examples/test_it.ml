let so2s = function
  | None -> "None"
  | Some s -> Printf.sprintf "Some(%S)" s

let vco2s = function
  | None -> "None"
  | Some (v, version) -> Printf.sprintf "Some(%S, %s)" v (so2s version)


open Lwt
type test_result =
  | Ok
  | Failed of string
  | Skipped

let show_test_result = function
  | Ok -> "Ok"
  | Failed s -> Printf.sprintf "Failed(%S)" s
  | Skipped -> "Skipped"

let lwt_test name (f:unit -> unit Lwt.t) : test_result Lwt.t=
  Lwt_log.debug_f "starting:%s" name >>= fun () ->
  let timeout = 300.
  (* overkill value, but:
     the simulator isn't that fast (FLUSH | WRITETHROUGH)
     and we sometimes need to test the real drives
     via ssh port forwarding.
   *)
  in
  Lwt.catch
     (fun () ->
      Lwt_unix.with_timeout timeout f
      >>= fun () ->
      Lwt.return Ok
     )
     (fun exn ->
      Lwt_log.info_f ~exn "failing:%s" name >>= fun () ->
      Lwt.return (Failed (Printexc.to_string exn))
     )
  >>= fun r ->
  Lwt_log.debug_f "end of :%s" name >>= fun () ->
  Lwt.return r

open Kinetic
module K = Make(BytesIntegration)
               
let test_get_non_existing client =
  K.get client "I do not exist?"
  >>= fun vo ->
  assert (vo = None);
  Lwt.return ()

let test_put_no_tag client =
  let key = "test_put_no_tag" in
  let v = key in
  let v_slice = key, 0, Bytes.length v in
  let synchronization = Some K.WRITEBACK in
  Lwt_io.printlf "drive[%S] <- Some %S%!" key v >>= fun () ->
  K.put
    client
    key v_slice
    ~db_version:None
    ~new_version:None
    ~forced:None
    ~tag:None
    ~synchronization
  >>= fun () ->
  Lwt.return ()
  

let test_put_empty_string client =
  let key = "test_put_empty_string" in
  let v = "" in
  let v_slice = v, 0, Bytes.length v in
  let tag = Some (Tag.Crc32 0x0l) in
  let synchronization = Some K.WRITEBACK in
  Lwt_io.printlf "drive[%S] <- Some %S%!" key v >>= fun () ->
  K.put
    client
    key v_slice
    ~db_version:None
    ~new_version:None
    ~forced:None
    ~tag
    ~synchronization
  >>= fun () ->
  K.get client key >>= fun vco ->
  let () = match vco with
    | None -> failwith "should have value"
    | Some (v2, version) ->
     begin
       assert (v2 = v);
       assert (version = Some "");
     end
  in
  Lwt.return_unit
  
let test_noop client = K.noop client

let batch_single_put client =
  K.start_batch_operation client >>= fun batch ->
  let v = "ZZZ" in
  let v_slice = v, 0 , Bytes.length v in
  let tag = K.make_sha1 v_slice in
  let pe = K.Entry.make
             ~key:"zzz"
             ~db_version:None
             ~new_version:(Some "ZZZ")
             (Some (v_slice,tag))
  in
  K.batch_put batch pe ~forced:(Some true) >>= fun () ->
  K.end_batch_operation batch >>= fun conn ->
  Lwt.return ()

let batch_test_put_delete client : unit Lwt.t=
  K.start_batch_operation client
  >>= fun batch ->
  let v = "XXX" in
  let v_slice = v,0,Bytes.length v in
  let tag = K.make_sha1 v_slice in
  let pe = K.Entry.make
             ~key:"xxx"
             ~db_version:None
             ~new_version:None
             (Some (v_slice, tag))
  in
  K.batch_put batch pe  ~forced:(Some true) >>= fun () ->

  let de = K.Entry.make
             ~key:"xxx"
             ~db_version:None
             ~new_version: None
             None
  in
  K.batch_delete batch de ~forced:(Some true) >>= fun () ->
  K.end_batch_operation batch >>= fun conn ->
  Lwt.return ()


let batch_delete_non_existing client =
  K.start_batch_operation client >>= fun batch ->
  let de =
    K.Entry.make
      ~key:"I do not exist"
      ~db_version:None
      ~new_version:None
      None
  in
  K.batch_delete batch de ~forced:(Some true) >>= fun () ->
  Lwt_log.debug_f "delete sent" >>= fun () ->
  K.end_batch_operation batch >>= fun (ok, conn) ->
  assert ok;
  Lwt_log.debug_f "end_batch sent" >>= fun () ->
  Lwt.return ()

let _add_put batch key v =
  let v_slice = v,0,Bytes.length v in
  let pe =
    let tag = K.make_sha1 v_slice in
    K.Entry.make
      ~key
      ~db_version:None
      ~new_version:None
      (Some (v_slice, tag))
  in
  K.batch_put batch pe ~forced:(Some true)

let batch_3_puts client : unit Lwt.t =
  K.start_batch_operation client
  >>= fun batch ->
  let make_key i = Printf.sprintf "batch_test_3_puts:key_%03i" i in
  let key0 = make_key 0 in
  let key1 = make_key 1 in
  let key2 = make_key 2 in
  _add_put batch key0 key0 >>= fun () ->
  _add_put batch key1 key1 >>= fun () ->
  _add_put batch key2 key2 >>= fun () ->
  K.end_batch_operation batch >>= fun (ok, conn) ->
  assert ok;
  Lwt.return_unit


let batch_too_fat client : unit Lwt.t =
  let make_key i = Printf.sprintf "batch_too_fat:key_%03i" i in
  let session = K.get_session client in
  let config = K.get_config session in
  let max = config.max_operation_count_per_batch in
  let n = max + 5 in
  let rec loop batch i =
    if i = n
    then Lwt.return_unit
    else
      begin
        let key = make_key 0 in
        _add_put batch key key >>= fun () ->
        loop batch (i+1)
      end
  in
  Lwt.catch
    (fun () ->
      K.start_batch_operation client >>= fun batch ->
      loop batch 0 >>= fun () ->
      K.end_batch_operation batch >>= fun (ok, conn) ->
      assert false
    )
    (fun exn ->
      Lwt_log.info_f ~exn "HIER" >>= fun () ->
     assert false
    )

let test_crc32 client =
  let key = "test_crc32_key" in
  let v = key in
  let v_slice = v,0,Bytes.length v in
  (*let tag = K.Crc32 0xEAE10D3Al in*)
  let tag = Tag.Crc32 0x0l in
  let synchronization = Some K.WRITEBACK in
  K.put client key v_slice
    ~db_version:None
    ~new_version:None
    ~forced:None
    ~tag:(Some tag)
    ~synchronization
  

let test_put_get_delete client =
  let rec loop i =
    if i = 400
    then Lwt.return ()
    else
      let key = Printf.sprintf "x_%05i" i  in
      let v = Printf.sprintf "value_%05i" i in
      let v_slice = v,0,Bytes.length v in
      let synchronization = Some K.WRITEBACK in
      Lwt_io.printlf "drive[%S] <- Some %S%!" key v >>= fun () ->
      let tag = K.make_sha1 v_slice in

      K.put
        client key v_slice
        ~db_version:None
        ~new_version:None
        ~forced:None
        ~tag:(Some tag)
        ~synchronization
      >>= fun () ->
      K.get client key >>= fun vco ->
      Lwt_io.printlf "drive[%S]=%s%!" key (vco2s vco) >>= fun () ->
      let () = match vco with
      | None -> failwith "should be present"
      | Some (value2, version) ->
         begin
           assert (v = value2);
           assert (version = Some "");
         end
      in
      K.delete_forced client key >>= fun () ->
      Lwt_io.printlf "deleted %S" key >>= fun () ->
      K.get client key >>= fun vco ->
      Lwt_io.printlf "drive[%S]=%s" key (vco2s vco) >>= fun () ->
      assert (vco = None);
      loop (i+1)
  in
  loop 0

let test_put_largish client =
  let key = "largish" in
  let v = Bytes.create 100_000 in
  let v_slice = v,0,Bytes.length v in
  let tag = K.make_sha1 v_slice in
  let synchronization = Some K.FLUSH in
  K.put client key v_slice
    ~new_version:None
    ~db_version:None
    ~forced:(Some true)
    ~synchronization
    ~tag:(Some tag)
  >>=fun () ->
  K.get client key >>= fun vco ->
  assert (vco <> None);
  Lwt.return ()

let test_put_version client =
  let key = "with_version" in
  K.delete_forced client key >>= fun () ->
  let v = "the_value" in
  let v_slice = v,0,Bytes.length v in
  let tag = K.make_sha1 v_slice in
  let version = Some "0" in
  let synchronization = Some K.FLUSH in
  K.put
    client key v_slice
    ~new_version:version
    ~db_version:None
    ~forced:(Some true)
    ~synchronization
    ~tag:(Some tag)
  >>= fun () ->
  K.get client key >>= fun vco ->
  Lwt_io.printlf "vco=%s" (vco2s vco) >>= fun () ->
  begin
    Lwt.catch
      (fun () ->
       let new_version = Some "1" in
       let v2 = "next_value" in
       let v2_slice = v2,0,Bytes.length v2 in
       let tag2 = K.make_sha1 v2_slice in
       K.put
         client key v2_slice
         ~db_version:new_version ~new_version
         ~forced:None
         ~synchronization
         ~tag:(Some tag2)
       >>= fun () ->
       Lwt.return false
      )
      (fun exn -> Lwt.return true)
  end
  >>= function
  | false -> Lwt.fail (Failure "bad behaviour")
  | true ->
     K.get client key >>= fun vco2 ->
     Lwt_io.printlf "vco2=%s" (vco2s vco2) >>= fun () ->
     Lwt.return ()

let fill client n =
  let synchronization = Some K.WRITEBACK in
  let rec loop i =
    if i = n
    then Lwt.return ()
    else
      let key = Printf.sprintf "x_%05i" i in
      let v = Printf.sprintf "value_%05i" i in
      let v_slice = v, 0, Bytes.length v in
      let tag = K.make_sha1 v_slice in
      begin
        if i mod 100 = 0 then Lwt_io.printlf "i:%i" i else Lwt.return ()
      end
      >>= fun ()->
      K.put
        client key v_slice
        ~db_version:None
        ~new_version:None
        ~forced:(Some true)
        ~synchronization
        ~tag:(Some tag)
      >>= fun () ->
      loop (i+1)
  in
  loop 0



let range_test client =
  fill client 1000 >>= fun () ->
  K.get_key_range
    client
    "x" true "y" true false 20
  >>= fun keys ->
  Lwt_io.printlf "[%s]\n%!" (String.concat "; " keys) >>= fun () ->
  assert (List.length keys = 20);
  assert (List.hd keys= "x_00000");
  Lwt.return ()

let range_test_reverse client =
  fill client 1000 >>= fun () ->
  (* note the order, which differs from the specs *)
  K.get_key_range client "x" true "y" true true 20
  >>= fun keys ->
  Lwt_io.printlf "[%s]\n%!" (String.concat "; " keys) >>= fun () ->
  assert (List.length keys = 20);
  assert (List.hd keys= "x_00999");
  Lwt.return ()


let get_capacities_test client =
  K.get_capacities client >>= fun (cap, fill_rate) ->
  Lwt_io.printlf "(%Li,%f)" cap fill_rate >>= fun () ->
  Lwt.return_unit


(*
let peer2peer_test session conn =
  let peer = "192.168.11.102", 8000, false in
  let operations = [
      ("x_00000", None);
      ("x_00010", None);
      ("x_00100", Some "y_00100");
    ]
  in
  K.p2p_push session conn peer operations
 *)

let maybe_init_ssl =
  let ok = ref false in
  (fun () ->
    if !ok then ()
    else
      begin
        Ssl_threads.init ();
        Ssl.init ~thread_safe:true ();
        ok := true
      end
  )

let make_socket_address h p = Unix.ADDR_INET(Unix.inet_addr_of_string h, p)

let ssl_connect ctx ip port =
  Lwt_log.debug_f "ssl_connect:(%s,%i)" ip port >>= fun () ->
  let sa = Unix.ADDR_INET(Unix.inet_addr_of_string ip, port) in
  let domain = Unix.domain_of_sockaddr sa in
  let socket = Lwt_unix.socket domain Unix.SOCK_STREAM 0  in
  Lwt_unix.connect socket sa >>= fun () ->
  Lwt_log.debug_f "connected" >>= fun () ->

  (*
        Ssl.set_verify ctx
          [Ssl.Verify_peer; Ssl.Verify_fail_if_no_peer_cert]
           (Some Ssl.client_verify_callback);
           Ssl.load_verify_locations ctx ca_cert "";
   *)

  Lwt_ssl.ssl_connect socket ctx >>= fun ssl_socket ->
  Lwt_log.debug_f "ssl_connect ok" >>= fun () ->
  Lwt.return ssl_socket

let make_client ?ctx ?secret ?cluster_version ?trace ~ip ~port =
  let sa = make_socket_address ip port in
  let domain = Unix.domain_of_sockaddr sa in
  match ctx with
  | None ->
     let socket = Lwt_unix.socket domain Unix.SOCK_STREAM 0 in
     Lwt_unix.connect socket sa >>= fun () ->
     let closer () =
       Lwt.catch
         (fun () -> Lwt_unix.close socket )
         (fun exn -> Lwt_log.info ~exn "during close")
     in
     let ssl_socket = Lwt_ssl.plain socket in
     K.wrap_socket ?secret ?cluster_version ?trace ssl_socket closer

  | Some ctx ->
     ssl_connect ctx ip port >>= fun ssl_socket ->
     let closer () =
       Lwt.catch
         (fun () -> Lwt_ssl.close ssl_socket)
         (fun exn -> Lwt_log.info ~exn "during close ")
     in
     K.wrap_socket ?trace ?secret ?cluster_version ssl_socket closer
         
let with_client ?ctx ?secret ?cluster_version ?trace ~ip ~port  f =
  make_client ?ctx ?secret ?cluster_version ?trace ~ip ~port
  >>= fun client ->
  Lwt.finalize
    (fun () -> f client)
    (fun () -> K.dispose client)
  
let run_with_client ip port trace ssl f =
  let ctx =
    if ssl then
      begin
        maybe_init_ssl ();
        let protocol = Ssl.TLSv1_2 in
        let ctx = Ssl.create_context protocol Ssl.Client_context in
        Some ctx
      end
    else
      None
  in

  let t =
    Lwt_log.debug_f
      "ip:%S port:%i trace:%b" ip port trace
    >>= fun () ->
    with_client ?ctx ~ip ~port ~trace f
  in
  Lwt_log.add_rule "*" Lwt_log.Debug;
  Lwt_main.run t


let get_info ip port trace ssl =
  run_with_client ip port trace ssl
    (fun client ->
      Lwt_io.printlf "config:%s" (client |> K.get_session |> K.get_config |> Config.show)
      >>= fun () ->
      Lwt.return_unit
    )


let instant_secure_erase ip port trace =

  let f client =
    let session = K.get_session client in
    let config = K.get_config session in
    Lwt_io.printlf "%s" (Config.show config) >>= fun () ->
    K.instant_secure_erase client >>= fun () ->
    Lwt.return_unit
  in

  let ssl = true in
  run_with_client ip port trace ssl f


let download_firmware ip port trace file_name =
  let f client =
    let session = K.get_session client in
    let config = K.get_config session in
    Lwt_io.printlf "%s" (Config.show config) >>= fun () ->
    Lwt_unix.stat file_name >>= fun stat ->
    let size = stat.Lwt_unix.st_size in
    let slod = Bytes.create size in
    Lwt_io.with_file
      ~mode:Lwt_io.input file_name
      (fun ic ->
        Lwt_io.read_into_exactly ic slod 0 size
      )
    >>= fun () ->
    Lwt_io.printlf "update has %i bytes%!" size >>= fun () ->
    let v_slice = (slod,0,size) in
    K.download_firmware client v_slice 
  in
  let ssl = true in
  run_with_client ip port trace ssl f

let run_tests ip port trace ssl filter =
  let f client =
    let run_tests tests =
      Lwt_list.map_s
        (fun (test_name, test) ->
          if filter = [] || List.mem test_name filter
          then
            begin
              lwt_test test_name (fun () -> test client)
              >>= fun r ->
              Lwt.return (test_name,r)
            end
          else
            Lwt.return (test_name, Skipped)
        )
        tests
    in
    run_tests [
        "get_non_existing",test_get_non_existing;
        "noop", test_noop;
        "put_get_delete", test_put_get_delete;
        "put_version", test_put_version;
        "put_empty_string", test_put_empty_string;
        "put_largish", test_put_largish;
        "range_test", range_test;
        "range_test_reverse", range_test_reverse;

        "batch_single_put", batch_single_put;
        "batch_put_delete", batch_test_put_delete;
        "batch_delete_non_existing", batch_delete_non_existing;
        "batch_3_puts", batch_3_puts;

        "crc32", test_crc32;
        "get_capacities", get_capacities_test;
        "batch_too_fat", batch_too_fat;
        (* "put_no_tag", test_put_no_tag; *)
        (*"peer2peer", peer2peer_test;*)
      ]
    >>= fun results ->
    Lwt_list.iter_s
      (fun (n, r) -> Lwt_io.printlf "%-32s => %s"  n (show_test_result r))
      results
  in
  run_with_client ip port trace ssl f


module Cli = struct
  open Cmdliner
  let ip =
    Arg.(value
         & opt string "::1"
         & info ["h";"host"] ~docv:"HOST" ~doc:"the host to connect with")

  let port default =
    let doc = "tcp $(docv)" in
    Arg.(value
         & opt int default
         & info ["p"; "port"] ~docv:"PORT" ~doc)

  let trace =
    Arg.(value
         & flag
         & info ["trace"]
                ~doc:"trace sent/received messages")

  let ssl =
    Arg.(value
         & flag
         & info ["ssl"]
                ~doc:"perform communication using ssl sockets")

  let filter =
    Arg.(value
         & opt_all string []
         & info ["filter"] ~doc:"run test(s) matching"
    )

  let file =
    Arg.(required
         & opt (some non_dir_file) None
         & info ["file"] ~docv:"FILE")


  let run_tests_cmd =
    let open Term in
    (pure run_tests
     $ ip
     $ port 8123
     $ trace
     $ ssl
     $ filter

    ),
    info
      "run-tests"
      ~doc:"runs tests"

  let instant_secure_erase_cmd =
    let open Term in
    (pure instant_secure_erase
     $ ip
     $ port 8443
     $ trace
    ),
    info
      "instant-secure-erase"
      ~doc:"erases all data from the drive. Warranty void. You have been warned"

  let download_firmware_cmd =
    let open Term in
    (pure download_firmware
     $ ip
     $ port 8443
     $ trace
     $ file
    ),
    info
      "download-firmware"
      ~doc:"flash new firmware on drive. Warranty void. You have been warned."

  let get_info_cmd =
    let open Term in
    (pure get_info
     $ ip
     $ port 8443
     $ trace
     $ ssl
    ),
    info "get-info"
      ~doc:"retrieve & dump some information about a drive"

  let default () =
    Printf.printf "an ocaml client for kinetic drives: tester & cli %!"

  let default_cmd =
    Term.(const default $ const ()),
    Term.info "" ~doc:"what's possible?"

end

let () =
  let open Cli in
  let open Cmdliner in
  let cmds =
    [run_tests_cmd;
     instant_secure_erase_cmd;
     download_firmware_cmd;
     get_info_cmd;
    ]
  in

  match Term.eval_choice default_cmd cmds with
  | `Error _ -> exit 1
  | _ -> exit 0
