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

const GCSFileIO = RemoteResourceIO{GCSResource}

function RemoteResourceIO{GCSResource}(gsurl::AbstractString; kwargs...)
    RemoteResourceIO(GCSResource(gsurl); kwargs...)
end

function RemoteResourceIO{GCSResource}(gsurl::AbstractString, client::GCSClient; kwargs...)
    RemoteResourceIO(GCSResource(gsurl, client); kwargs...)
end

function RemoteResourceIO{GCSResource}(client::GCSClient; kwargs...)
    gsurl -> GCSResource(gsurl, client; kwargs...)
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
