(*
  Copyright (C) iNuron - info@openvstorage.com
  This file is part of Open vStorage. For license information, see <LICENSE.txt>
 *)

open Kinetic_util

module Config = struct
    type t = {
        vendor: string;
        model:string;
        serial_number: string;
        world_wide_name: string;
        version: string;
        ipv4_addresses : string list;
        max_key_size: int;
        max_value_size: int;
        max_version_size: int;
        max_tag_size: int;
        max_connections: int;
        max_outstanding_read_requests: int;
        max_outstanding_write_requests: int;
        max_message_size: int;
        max_key_range_count: int;
        max_operation_count_per_batch: int option;
        (* max_batch_count_per_device: int; *)
        timeout : float;
      }

    let make ~vendor ~world_wide_name ~model
             ~serial_number
             ~version
             ~ipv4_addresses
             ~max_key_size
             ~max_value_size
             ~max_version_size
             ~max_tag_size
             ~max_connections
             ~max_outstanding_read_requests
             ~max_outstanding_write_requests
             ~max_message_size
             ~max_key_range_count
             ~max_operation_count_per_batch
             ~timeout
             (* ~max_batch_count_per_device *)
      = {
        vendor;
        model;
        serial_number;
        world_wide_name;
        version;
        ipv4_addresses;
        max_key_size;
        max_value_size;
        max_version_size;
        max_tag_size;
        max_connections;
        max_outstanding_read_requests;
        max_outstanding_write_requests;
        max_message_size;
        max_key_range_count;
        max_operation_count_per_batch;
        (* max_batch_count_per_device; *)
        timeout;
      }

    let show t =
      let buffer = Buffer.create 128 in
      let add x = Printf.kprintf (fun s -> Buffer.add_string buffer s) x in
      add "Config {";
      add " version: %S;" t.version;
      add " ipv4_addresses: [%s]" (String.concat ";" t.ipv4_addresses);
      add " wwn:%S;" t.world_wide_name;
      add " serial_number:%S;" t.serial_number;
      add " max_key_size:%i;" t.max_key_size;
      add " max_value_size:%i;" t.max_value_size;
      add " max_version_size:%i;" t.max_version_size;
      add " max_tag_size:%i;" t.max_tag_size;
      add " max_connections:%i;" t.max_connections;
      add " max_outstanding_read_requests:%i;" t.max_outstanding_read_requests;
      add " max_oustranding_write_requests:%i;" t.max_outstanding_write_requests;
      add " max_message_size:%i;" t.max_message_size;
      add " max_operation_count_per_batch:%s;" (show_option string_of_int t.max_operation_count_per_batch);
      add "}";
      Buffer.contents buffer
end
