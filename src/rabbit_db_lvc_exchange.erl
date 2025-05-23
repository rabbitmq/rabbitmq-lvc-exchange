-module(rabbit_db_lvc_exchange).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("khepri/include/khepri.hrl").
-include("rabbit_lvc_plugin.hrl").

-export([setup_schema/0,
         get/2,
         insert/3,
         delete/0,
         delete/1
        ]).

-export([khepri_lvc_path/1, khepri_lvc_path/2]).

-rabbit_mnesia_tables_to_khepri_db(
   [{?LVC_TABLE, rabbit_lvc_plugin_m2k_converter}]).

setup_schema() ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() -> setup_schema_in_mnesia() end,
          khepri => fun() -> ok end}
    ).

setup_schema_in_mnesia() ->
    _ = mnesia:create_table(?LVC_TABLE,
                            [{attributes, record_info(fields, cached)},
                             {record_name, cached},
                             {type, set},
                             {disc_copies, [node()]}]),
    _ = mnesia:add_table_copy(?LVC_TABLE, node(), disc_copies),
    rabbit_table:wait([?LVC_TABLE]),
    ok.

get(XName, RoutingKey) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_in_mnesia(XName, RoutingKey) end,
        khepri => fun() -> get_in_khepri(XName, RoutingKey) end}
    ).

get_in_mnesia(XName, RoutingKey) ->
    case mnesia:dirty_read(
           ?LVC_TABLE,
           #cachekey{exchange = XName,
                     routing_key = RoutingKey }) of
        [] ->
            not_found;
        [#cached{content = Msg}] ->
            Msg
    end.

get_in_khepri(XName, RoutingKey) ->
    case rabbit_khepri:get(khepri_lvc_path(XName, RoutingKey)) of
        {ok, Msg} ->
            Msg;
        _ ->
            not_found
    end.

insert(XName, RKs, Msg) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> insert_in_mnesia(XName, RKs, Msg) end,
        khepri => fun() -> insert_in_khepri(XName, RKs, Msg) end}
    ).

insert_in_mnesia(XName, RKs, Msg) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              [mnesia:write(?LVC_TABLE,
                            #cached{key = #cachekey{exchange=XName,
                                                    routing_key=K},
                                    content = Msg},
                            write) ||
               K <- RKs]
      end),
    ok.

insert_in_khepri(XName, RKs, Msg) ->
    [ rabbit_khepri:put(khepri_lvc_path(XName, RK), Msg)
      || RK <- RKs ],
    ok.

delete() ->
    rabbit_khepri:handle_fallback(
        #{mnesia => fun() -> _ = mnesia:delete_table(?LVC_TABLE), ok end,
          khepri => fun() -> _ = rabbit_khepri:delete([rabbit_exchange_type_lvc]), ok end}
    ).

delete(Exchange) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_in_mnesia(Exchange) end,
        khepri => fun() -> delete_in_khepri(Exchange) end}
    ).

delete_in_mnesia(#exchange{ name = Name }) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              [mnesia:delete(?LVC_TABLE, K, write) ||
               #cached{ key = K } <-
               mnesia:match_object(?LVC_TABLE,
                                   #cached{key = #cachekey{
                                                    exchange = Name, _ = '_' },
                                           _ = '_'}, write)]
      end),
    ok;
delete_in_mnesia(_X) ->
	ok.

delete_in_khepri(#exchange{ name = Name }) ->
    rabbit_khepri:delete(khepri_lvc_path(Name)),
    ok;
delete_in_khepri(_X) ->
    ok.

khepri_lvc_path() ->
    [rabbit_exchange_type_lvc].

khepri_lvc_path(#resource{virtual_host = VHost, name = Name}) ->
    khepri_lvc_path() ++ [VHost, Name].

khepri_lvc_path(Exchange, RK) ->
    khepri_lvc_path(Exchange) ++ [RK].
