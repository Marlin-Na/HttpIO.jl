module HttpIO

import HTTP
import TranscodingStreams

######## RemoteResource Interface  #######################

abstract type RemoteResource end

function Base.length(x::RemoteResource) end

function Base.getindex(x::RemoteResource, range::UnitRange{<:Integer}) end

#### HttpResource

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

function Base.length(x::HttpResource)
    @assert x.size >= 0
    x.size
end

function Base.getindex(x::HttpResource, range::UnitRange{<:Integer})
    s = first(range)
    e = last(range)
    @assert 1 <= s <= e <= length(x)
    rangestr = "bytes=$(s-1)-$(e-1)" 
    headers = ["Range" => rangestr]
    res = HTTP.get(x.url; retry=true, redirect=false, headers=headers)
    @assert res.status == 206
    @assert HTTP.header(res, "Content-Length") == string(e - s + 1)
    HTTP.body(res)
end


######## RemoteResourceIO          #######################


mutable struct RemoteResourceIO{T} <: IO
    resource::T
    offset::Int64
    stat::NamedTuple{(:nreq, :size, :time), Tuple{Int64, Int64, Float64}}
    function RemoteResourceIO{T}(resource) where {T}
        new(resource, 0, (nreq=0, size=0, time=0.0))
    end
end

const HttpFileIO = RemoteResourceIO{HttpResource}

function Base.position(io::RemoteResourceIO)
    io.offset
end

function Base.seek(io::RemoteResourceIO, pos::Integer)
    io.offset = pos
    io
end

function Base.seekstart(io::RemoteResourceIO)
    seek(io, 0)
end

function Base.seekend(io::RemoteResourceIO)
    seek(io, length(io))
end

function Base.eof(io::RemoteResourceIO)
    position(io) >= length(io.resource)
end

function Base.close(io::RemoteResourceIO)
    return nothing
end
function Base.isopen(io::RemoteResourceIO)
    return true
end

function Base.bytesavailable(io::RemoteResourceIO)
    return 0
end

# Each call to unsafe_read will send one http request
function Base.unsafe_read(io::RemoteResourceIO, output::Ptr{UInt8}, nbytes::UInt)
    start = position(io) + 1
    stop = min(start + nbytes - 1, length(io.resource))
    having_extra = (start + nbytes - 1) > length(io.resource)
    # read http response
    time = @elapsed begin
        data = io[start:stop]
        @assert length(data) == (stop - start + 1)
        GC.@preserve data unsafe_copyto!(output, pointer(data), length(data))
    end
    # set offset and stat
    new_stat = NamedTuple{(:nreq, :size, :time), Tuple{Int64, Int64, Float64}}(
        nreq=io.stat.nreq + 1, size=io.stat.size + length(data), time=time)
    io.offset = io.offset + length(data)
    io.stat = new_stat
    having_extra && throw(EOFError())
    return nothing
end

end
