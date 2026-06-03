-module(sofia_multitenant).
-export([register_tenant_service/3, deregister_tenant_service/3, discover_tenant_service/2]).

%% @doc Registers a service process for a specific tenant.
register_tenant_service(TenantId, ServiceType, Pid) ->
    sofia_registry:register_service({tenant, TenantId, ServiceType}, Pid).

%% @doc Deregisters a tenant-specific service process.
deregister_tenant_service(TenantId, ServiceType, Pid) ->
    sofia_registry:deregister_service({tenant, TenantId, ServiceType}, Pid).

%% @doc Discovers a service process for a tenant. If no tenant-specific instance
%% is registered, it automatically falls back to a global/shared instance.
discover_tenant_service(TenantId, ServiceType) ->
    case sofia_registry:discover({tenant, TenantId, ServiceType}) of
        {ok, Pid} ->
            {ok, Pid};
        {error, no_service_available} ->
            %% Fallback to global/shared instance
            sofia_registry:discover({tenant, global, ServiceType})
    end.
