# Google cloud storage resource

import .PoorGCloudAuth: GCSClient
import .PoorGCloudAuth

struct GCSResource <: RemoteResource
    gsurl::String
    client::GCSClient
    size::Int64
    function GCSResource(gsurl, client)
        size = PoorGCloudAuth.fetch_object_size(gsurl, client)
        resource = new(gsurl, client, size)
        resource
    end
end

function GCSFileIO(gsurl::AbstractString)
    RemoteResourceIO(GCSResource(gsurl))
end

function GCSFileIO(gsurl::AbstractString, client::GCSClient)
    RemoteResourceIO(GCSResource(gsurl, client))
end

function GCSResource(client::GCSClient)
    gsurl -> GCSResource(gsurl, client)
end

function GCSResource(gsurl::AbstractString)
    GCSResource(gsurl, GCSClient())
end

function resc_length(x::GCSResource)
    x.size
end

function resc_fetch(x::GCSResource, range::UnitRange{<:Integer})
    PoorGCloudAuth.fetch_gcs(x.gsurl, x.client; range = range)
end

function resc_suggest_bufsize(x::GCSResource)
    # TODO: benchmark the performance against buffer size
    Int(1 * 1024 * 1024) # 1 MB
end
