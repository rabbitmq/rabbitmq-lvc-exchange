%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
-module(rabbit_lvc_plugin).

-include("rabbit_lvc_plugin.hrl").

-export([setup_schema/0, disable_plugin/0]).

-rabbit_boot_step({?MODULE,
                   [{description, "last-value cache exchange type"},
                    {mfa, {rabbit_lvc_plugin, setup_schema, []}},
                    {mfa, {rabbit_registry, register, [exchange, <<"x-lvc">>, rabbit_exchange_type_lvc]}},
                    {cleanup, {?MODULE, disable_plugin, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

%% private

setup_schema() ->
    rabbit_db_lvc_exchange:setup_schema().

disable_plugin() ->
    rabbit_registry:unregister(exchange, <<"x-lvc">>),
    rabbit_db_lvc_exchange:delete().
