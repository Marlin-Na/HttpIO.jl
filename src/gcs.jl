# Google cloud storage resource

import .PoorGCloudAuth: GCSClient
import .PoorGCloudAuth

struct GCSResource
    gsurl::String
    client::GCSClient
    size::Int64
    function GCSResource(gsurl, client)
        ans = new(gsurl, client)
        init_resource!(ans)
        ans
    end
end

function init_resource!(resource::GCSResource)
    resource.size = PoorGCloudAuth.fetch_object_size(resource.gsurl, resource.client)
    resource
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
    PoortGCloudAuth.fetch_gcs(x, x.client; range = range)
end

function resc_suggest_bufsize(x::GCSResource)
    # TODO: benchmark the performance against buffer size
    Int(1 * 1024 * 1024) # 1 MB
end
