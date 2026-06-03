# Chapter 8: External Edge Gateway with Cowboy

This chapter describes how to expose SOFIA's internal actor-based services to external clients using an HTTP/JSON Edge Gateway. We leverage Cowboy as the high-performance HTTP server and JSX for JSON encoding/decoding.

---

## 1. Architectural Overview

The HTTP Edge Gateway acts as a protocol bridge between the outside web (HTTP/JSON) and internal Erlang actors (Process Groups and Messages). 

```
[ External Client ]
        │ (POST /api/v1/service/calc_service)
        ▼
┌──────────────────────────────┐
│  sofia_http_handler (Cowboy) │
└──────────────┬───────────────┘
               │ 1. Parse JSON & Normalize keys to atoms
               │ 2. Match Method/Payload
               ▼
┌──────────────────────────────┐
│      sofia_client_stub       │
└──────────────┬───────────────┘
               │ 3. Discover target Pid
               │ 4. Fetch contract & Validate schema locally
               ▼
┌──────────────────────────────┐
│        Target Service        │
└──────────────────────────────┘
```

---

## 2. Ingress Configuration

The Cowboy server listener starts automatically when the `sofia_gateway` gen_server is initialized. The port is configurable via application environment variables (defaulting to `8080`).

### Router Compilation
Cowboy routes incoming POST requests to `sofia_http_handler` using the following path template:
```
/api/v1/service/:service_name
```
Where `:service_name` maps to the registered service type atom (e.g., `calc_service`).

---

## 3. JSON Payload Ingestion & Key Normalization

Erlang maps typically use atom keys for efficiency and clarity (e.g., `#{a => 10, b => 20}`). However, JSON decoders decode keys as binary strings (e.g., `#{<<"a">> => 10, <<"b">> => 20}`).

To bridge this mismatch without risking atom table exhaustion, the HTTP handler performs recursive key normalization:
1. It attempts to find an existing atom for the binary key using `binary_to_existing_atom/2`.
2. If the atom does not yet exist in the VM, it falls back to `binary_to_atom/2`.
3. This normalization ensures the request payload can be successfully matched against the service contract schema.

---

## 4. HTTP POST Request Contract Invocation

External clients invoke a contract method by providing a POST request formatted with a `method` string and a `payload` map:

### Request Format
```json
{
  "method": "add",
  "payload": {
    "a": 10,
    "b": 25
  }
}
```

### Handler Processing Loop
```erlang
Result = case {maps:find(method, Normalized), maps:find(payload, Normalized)} of
    {{ok, MethodStr}, {ok, PayloadMap}} when is_map(PayloadMap) andalso is_binary(MethodStr) ->
        MethodAtom = list_to_atom(binary_to_list(MethodStr)),
        sofia_client_stub:call_service(ServiceAtom, {MethodAtom, PayloadMap});
    ...
```

The gateway intercepts validation errors and translates them into appropriate HTTP status codes:
- **200 OK**: The request passed validation, was executed by the actor, and returned the reply.
- **400 Bad Request**: The request was malformed or failed the contract schema checks (e.g., missing parameter or type mismatch).
- **500 Internal Server Error**: The service was unreachable, timed out, or encountered an unhandled exception.

---

## 5. API Usage Examples

### Example A: Successful Execution (200 OK)

**Request:**
```bash
curl -X POST http://localhost:8080/api/v1/service/calc_service \
  -H "Content-Type: application/json" \
  -d '{"method": "add", "payload": {"a": 100, "b": 250}}'
```

**Response:**
```json
{
  "status": "success",
  "result": 350
}
```

### Example B: Missing Parameter Validation Failure (400 Bad Request)

**Request:**
```bash
curl -X POST http://localhost:8080/api/v1/service/calc_service \
  -H "Content-Type: application/json" \
  -d '{"method": "add", "payload": {"a": 100}}'
```

**Response:**
```json
{
  "status": "error",
  "reason": "contract_validation_failed",
  "details": "Missing parameter: b"
}
```

### Example C: Type Mismatch Validation Failure (400 Bad Request)

**Request:**
```bash
curl -X POST http://localhost:8080/api/v1/service/calc_service \
  -H "Content-Type: application/json" \
  -d '{"method": "add", "payload": {"a": 100, "b": "invalid_type"}}'
```

**Response:**
```json
{
  "status": "error",
  "reason": "contract_validation_failed",
  "details": "Type mismatch for b. Expected integer, got <<\"invalid_type\">>"
}
```

---

## 6. Federated Cryptographic Access Control

SOFIA supports robust federated authentication and access control through **HMAC-SHA256 request signatures** and **replay attack protection**.

### Enforcing Security in Service Contracts
To require authentication for a service, specify `security => hmac` in the service's registered contract:

```erlang
Contract = #{
    security => hmac,
    methods => #{
        add => #{
            input_schema => #{
                a => integer,
                b => integer
            }
        }
    }
}.
```

### Authentication Protocol
When a service is marked secure, the gateway intercepts requests and expects the following HTTP headers:
1. `X-Sofia-Client-Id`: A unique identifier for the calling client.
2. `X-Sofia-Timestamp`: Unix epoch timestamp representing when the request was constructed.
3. `X-Sofia-Signature`: HMAC-SHA256 signature represented in hex, computed as:
   `HMAC_SHA256(Secret, ClientId ++ "." ++ Timestamp ++ "." ++ Body)`

### Replay Attack Prevention
The gateway computes the difference between the current system time and the timestamp in the `X-Sofia-Timestamp` header. If the skew exceeds `300 seconds` (5 minutes), the request is rejected with `403 Forbidden` (`replay_or_skewed_clock`).

### API Examples for Secure Services

#### Example A: Missing Authentication Headers (401 Unauthorized)

```bash
curl -X POST http://localhost:8080/api/v1/service/secure_service \
  -H "Content-Type: application/json" \
  -d '{"method": "add", "payload": {"a": 10, "b": 20}}'
```

**Response:**
```json
{
  "status": "error",
  "reason": "missing_auth_headers"
}
```

#### Example B: Invalid Signature (403 Forbidden)

```bash
curl -X POST http://localhost:8080/api/v1/service/secure_service \
  -H "Content-Type: application/json" \
  -H "X-Sofia-Client-Id: client_123" \
  -H "X-Sofia-Signature: invalid_signature" \
  -H "X-Sofia-Timestamp: 1717366400" \
  -d '{"method": "add", "payload": {"a": 10, "b": 20}}'
```

**Response:**
```json
{
  "status": "error",
  "reason": "forbidden",
  "details": "invalid_signature"
}
```

#### Example C: Valid Authenticated Call (200 OK)

Compute the signature on the client:
```erlang
Timestamp = integer_to_binary(erlang:system_time(second)),
{ok, SignatureHex} = sofia_auth:sign_payload(<<"client_123">>, Timestamp, Body).
```

Include the headers:
```bash
curl -X POST http://localhost:8080/api/v1/service/secure_service \
  -H "Content-Type: application/json" \
  -H "X-Sofia-Client-Id: client_123" \
  -H "X-Sofia-Signature: 6e9d8e7b..." \
  -H "X-Sofia-Timestamp: 1717366400" \
  -d '{"method": "add", "payload": {"a": 10, "b": 20}}'
```

**Response:**
```json
{
  "status": "success",
  "result": 30
}
```

