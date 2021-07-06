# utils for running gcloud command, reading ~/.config/gcloud directory or
# interacting with google cloud api.

"""
    read_access_token()
    read_refresh_token()

Obtain google cloud access token or refresh token with
`gcloud auth print-access-token` or `gcloud auth print-refresh-token`
command. The access token usually expires after one hour, the refresh
token can be used to obtain new access token.
"""
read_access_token, read_refresh_token

function read_access_token()
    token = read(`gcloud auth --quiet print-access-token`, String)
    token = rstrip(token)
    token
end

function read_refresh_token()
    token = read(`gcloud auth --quiet print-refresh-token`, String)
    token = rstrip(token)
    token
end

"""
    read_active_account()

Read active account name from `gcloud auth list` command.
"""
function read_active_account()
    account = read(`gcloud --quiet auth list`, String)
    account = rstrip(account)
    account = split(account, "\n")
    account = filter(startswith("*"), account)
    length(account) != 1 && error("error finding active account with 'gcloud auth list'")
    account = account[1]
    account = replace(account, r"\* *" => "")
    account
end

"""
    read_account_state(account)

Read account auth states as a dictionary for a given account from
`gcloud auth describe` command.
"""
function read_account_desc(account)
    info = read(`gcloud auth describe $account`, String)
    info = rstrip(info)
    info = split(info, "\n")
    info = filter(contains(": "), info)
    info = map(x -> split(x, ": "), info)
    Dict{String,String}(info)
end

"""
    read_application_default_credentials()

Read content from "~/.config/gcloud/application_default_credentials.json".
It contains client_id, client_secret and refresh_token.
"""
function read_application_default_credentials()
    json = open(joinpath(homedir(), ".config/gcloud/application_default_credentials.json")) do f
        read(f, String)
    end
    dict = JSON.parse(json)
    dict
end

"""
    fetch_access_token_info(token)

Get access token information (e.g. expiration time) by querying google api.
"""
function http_access_token_info(token)
    query = "https://www.googleapis.com/oauth2/v1/tokeninfo?access_token=" * token
    res = HTTP.get(query)
    @assert HTTP.status(res) == 200
    json = String(HTTP.body(res))
    dict = JSON.parse(json)
    dict
end

"""
    http_reauth_access_token(;refresh_token, client_id, client_secret)

Obtain a new access token with refresh_token, client_id and client_secret from google api.
"""
# FIXME: this is not done right, check
# https://github.com/google/google-reauth-python/blob/master/google_reauth/reauth.py#L150-L199
# https://github.com/googleapis/google-auth-library-python/blob/master/google/oauth2/credentials.py#L207-L254
function http_reauth_access_token(;refresh_token, client_id, client_secret)
    params = [
        "client_id" => client_id,
        "client_secret" => client_secret,
        "grant_type" => "refresh_token",
        "refresh_token" => refresh_token,
        "scope" => ["https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/accounts.reauth"]
    ]
    res = HTTP.post("https://www.googleapis.com/oauth2/v4/token"; query = params, redirect=false)
    @assert HTTP.status(res) == 200
    json = String(HTTP.body(res))
    dict = JSON.parse(json)
    dict
end

function parse_gsurl(gsurl)
    @assert startswith(gsurl, "gs://")
    gsurl = gsurl[(length("gs://") + 1):end]
    bucket = split(gsurl, "/")[1]
    path = gsurl[(length(bucket) + 2):end]
    bucket, path
end

function fetch_gcs(gsurl::AbstractString, access_token::AbstractString; range=nothing)
    (bucket, path) = parse_gsurl(gsurl)
    api = "https://$bucket.storage-download.googleapis.com/$path"
    headers = [
        "Authorization" => "Bearer " * access_token
    ]
    if !isnothing(range)
        @assert 1 <= first(range) <= last(range)
        push!(headers, "Range" => "bytes=$(first(range)-1)-$(last(range)-1)")
    end
    res = HTTP.get(api; retry=true, redirect=false, headers=headers)
    !isnothing(range) ? (@assert HTTP.status(res) == 206) : (@assert HTTP.status(res) == 200)
    return HTTP.body(res)
end

function fetch_object_info(gsurl::AbstractString, access_token::AbstractString)
    (bucket, path) = parse_gsurl(gsurl)
    path = HTTP.escape(path)
    api = "https://storage.googleapis.com/storage/v1/b/$bucket/o/$path"
    headers = [
        "Authorization" => "Bearer " * access_token
    ]
    res = HTTP.get(api; retry=true, redirect=false, headers=headers)
    @assert HTTP.status(res) == 200
    JSON.parse(String(HTTP.body(res)))
end
