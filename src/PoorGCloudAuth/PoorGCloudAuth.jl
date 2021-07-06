"""
Temporary solution for gaining google cloud access using access token
obtained from `gcloud` command. It assumes that Google Cloud SDK is
installed and authorized with `gcloud auth login --update-adc` command.
"""
module PoorGCloudAuth

import HTTP
import JSON
import Dates

include("utils.jl")

"""
    AccessToken()

Struct representing the access token and associated refresh token for google cloud api.
By default it will read refresh token from ~/.config/gcloud/application_default_credentials.json
and use it to obtain a short-lived access token.
"""
struct AccessToken
    refresh_token::String
    client_id::String
    client_secret::String
    access_token::String
    expiry::Dates.DateTime
    function AccessToken(refresh_token::String, client_id::String, client_secret::String, access_token::String, expiry::Dates.DateTime)
        new(refresh_token, client_id, client_secret, access_token, expiry)
    end
    function AccessToken(refresh_token::String, client_id::String, client_secret::String)
        partial = new(refresh_token, client_id, client_secret)
        reauth(partial)
    end
    function AccessToken()
        cred = read_application_default_credentials()
        AccessToken(cred["refresh_token"], cred["client_id"], cred["client_secret"])
    end
end

"""
Obtain a new access token from google cloud api with the refresh token.
"""
reauth, reauth!

"""
Get expiration time of the access token.
"""
expiry

"""
Download data from google bucket.
"""
fetch_gcs

"""
    fetch_object_info(gsurl, cred)
    fetch_object_size(gsurl, cred)

Obtain all meta data or object size for a google bucket object.
"""
fetch_object_info, fetch_object_size

function reauth(token::AccessToken)
    new = http_reauth_access_token(;refresh_token = token.refresh_token, client_id = token.client_id, client_secret = token.client_secret)
    expiry = Dates.now() + Dates.Second(new["expires_in"])
    access_token = new["access_token"] # also has new["scope"] and new["token_type"]
    AccessToken(token.refresh_token, token.client_id, token.client_secret, access_token, expiry)
end

function expiry(token::AccessToken)
    token.expiry
end

function fetch_gcs(gsurl::AbstractString, access_token::AccessToken; range=nothing)
    fetch_gcs(gsurl, access_token.access_token; range=range)
end

function fetch_object_info(gsurl::AbstractString, access_token::AccessToken)
    fetch_object_info(gsurl, access_token.access_token)
end

"""
Client that holds google cloud credentials. It manages to create short-lived access token from refresh token.
"""
mutable struct GCSClient
    token::AccessToken
    expiry_limit::Dates.Second
    lock::ReentrantLock
    function GCSClient(token::AccessToken)
        new(token, Dates.Second(Dates.Minute(30)), ReentrantLock())
    end
end

function GCSClient()
    GCSClient(AccessToken())
end

function reauth!(client::GCSClient)
    lock(client.lock)
    try
        time_now = Dates.now()
        # obtain new access token if time passed limit
        if time_now > expiry(client) - client.expiry_limit
            client.token = reauth(client.token)
        end
    finally
        unlock(client.lock)
    end
    client
end

function expiry(client::GCSClient)
    expiry(client.token)
end

"""
Obtain access token from the client.
"""
function accesstoken(client::GCSClient)
    reauth!(client) # reauth if needed
    lock(client.lock) do
        client.token
    end
end

function fetch_gcs(gsurl::AbstractString, client::GCSClient; range=nothing)
    fetch_gcs(gsurl, accesstoken(client); range=range)
end

function fetch_object_info(gsurl::AbstractString, client::GCSClient)
    fetch_object_info(gsurl, accesstoken(client))
end

function fetch_object_size(gsurl::AbstractString, cred)
    parse(Int, fetch_object_info(gsurl, cred)["size"])
end

end
