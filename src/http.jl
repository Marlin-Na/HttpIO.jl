
#### HttpResource

# TODO: benchmark the performance against buffer size

mutable struct HttpResource <: RemoteResource
    url::String
    size::Int
    function HttpResource(url)
        ans = new(url, -1)
        init_resource!(ans)
        ans
    end
end

function init_resource!(resource::HttpResource)
    res = HTTP.head(resource.url; redirect=false, retry=true)
    @assert res.status == 200

    # check server support Range header
    r = HTTP.header(res, "Accept-Ranges")
    r != "bytes" && error("Seems that the web server does not support 'Range' header")

    # get file size
    r = HTTP.header(res, "Content-Length")
    r == "" && error("Missing Content-Length header")
    resource.size = parse(Int, r)
    resource
end

function resc_length(x::HttpResource)
    @assert x.size >= 0
    x.size
end

function resc_fetch(x::HttpResource, range::UnitRange{<:Integer})
    s = first(range)
    e = last(range)
    @assert 1 <= s <= e <= resc_length(x)
    rangestr = "bytes=$(s-1)-$(e-1)" 
    headers = ["Range" => rangestr]
    res = HTTP.get(x.url; retry=true, redirect=false, headers=headers)
    @assert res.status == 206
    @assert HTTP.header(res, "Content-Length") == string(e - s + 1)
    HTTP.body(res)
end

const HttpFileIO = RemoteResourceIO{HttpResource}

function RemoteResourceIO{HttpResource}(url::AbstractString; kwargs...)
    RemoteResourceIO{HttpResource}(HttpResource(url); kwargs...)
end
