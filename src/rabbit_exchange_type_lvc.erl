%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
-module(rabbit_exchange_type_lvc).
-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("rabbit/include/mc.hrl").
-include("rabbit_lvc_plugin.hrl").

-behaviour(rabbit_exchange_type).

-export([description/0, serialise_events/0, route/3]).
-export([validate/1, validate_binding/2,
         create/2, recover/2, delete/2, policy_changed/2,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).
-export([info/1, info/2]).

info(_X) -> [].
info(_X, _) -> [].

description() ->
    [{name, <<"x-lvc">>},
     {description, <<"Last-value cache exchange.">>}].

serialise_events() -> false.

route(#exchange{name = Name}, Msg, _Opts) ->
    RKs = mc:routing_keys(Msg),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              [mnesia:write(?LVC_TABLE,
                            #cached{key = #cachekey{exchange=Name,
                                                    routing_key=K},
                                    content = Msg},
                            write) ||
               K <- RKs]
      end),
    rabbit_router:match_routing_key(Name, RKs).

validate(_X) -> ok.
validate_binding(_X, _B) -> ok.
create(_Serial, _X) -> ok.
recover(_X, _Bs) -> ok.

delete(none, #exchange{ name = Name }) ->
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
delete(_Serial, _X) ->
	ok.

policy_changed(_X1, _X2) -> ok.

add_binding(none,
            #exchange{name = XName },
            #binding{key = RoutingKey,
                     destination = #resource{kind = queue} = QName}) ->
    _ = case rabbit_amqqueue:lookup(QName) of
            {error, not_found} ->
                destination_not_found_error(QName);
            {ok, Q} ->
                case get_msg_from_cache(XName, RoutingKey) of
                    not_found ->
                        ok;
                    Msg ->
                        rabbit_queue_type:deliver([Q], Msg, #{}, stateless)
                end
        end,
    ok;
add_binding(none,
            #exchange{name = XName},
            #binding{key = RoutingKey,
                     destination = #resource{kind = exchange} = DestName}) ->
    _ = case rabbit_exchange:lookup(DestName) of
            {error, not_found} ->
                destination_not_found_error(DestName);
            {ok, X} ->
                case get_msg_from_cache(XName, RoutingKey) of
                    not_found ->
                        ok;
                    Msg ->
                        rabbit_queue_type:publish_at_most_once(X, Msg)
                end
        end,
    ok;
add_binding(_Serial, _X, _B) ->
    ok.

remove_bindings(_Serial, _X, _Bs) -> ok.

assert_args_equivalence(X, Args) ->
    rabbit_exchange_type_direct:assert_args_equivalence(X, Args).

-spec get_msg_from_cache(rabbit_types:exchange_name(),
                         rabbit_types:routing_key()) -> mc:state() | not_found.
get_msg_from_cache(XName, RoutingKey) ->
    case mnesia:dirty_read(
           ?LVC_TABLE,
           #cachekey{exchange = XName,
                     routing_key = RoutingKey }) of
        [] ->
            not_found;
        [#cached{content = Msg}] ->
            mc:set_annotation(?ANN_ROUTING_KEYS, [RoutingKey], Msg)
    end.

-spec destination_not_found_error(rabbit_types:r('exchange' | 'queue')) -> no_return().
destination_not_found_error(DestName) ->
    rabbit_misc:protocol_error(
      internal_error,
      "could not find destination '~ts'",
      [rabbit_misc:rs(DestName)]).
