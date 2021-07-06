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

Obtain a short-lived access token for google cloud api with
`gcloud auth print-access-token` command.
"""
struct AccessToken
    access_token::String
    expiry::Dates.DateTime
    function AccessToken(token::AbstractString)
        info = http_access_token_info(token)
        expiry = Dates.now() + Dates.Second(info["expires_in"])
        new(token, expiry)
    end
    function AccessToken()
        token = read_access_token()
        AccessToken(token)
    end
end

"""
Obtain a new access token if needed.
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
    # for now, we won't use refresh_token, instead just rerunning "gcloud auth print-access-token"
    AccessToken()
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
