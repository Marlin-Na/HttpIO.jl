module HttpIO

export HttpFileIO
export GCSFileIO
export GCSClient

import HTTP
import TranscodingStreams
include("PoorGCloudAuth/PoorGCloudAuth.jl")

######## RemoteResource Interface  #######################

abstract type RemoteResource end

function resc_length(x::RemoteResource) error("not implemented") end

function resc_fetch(x::RemoteResource, range::UnitRange{<:Integer}) error("not implemented") end


######## RemoteResourceIO          #######################

## The incomplete, unbuffered IO.
## It only implements TranscodingStreams.readdata! method. TranscodingStreams.NoopStream
## is then used to provide the buffer and related IO methods.

mutable struct DummyRemoteResourceIO{T} <: IO
    resource::T
    offset::Int64
    function DummyRemoteResourceIO(resource::RemoteResource)
        new{typeof(resource)}(resource, 0)
    end
end

function Base.position(io::DummyRemoteResourceIO)
    io.offset
end

function Base.seek(io::DummyRemoteResourceIO, pos::Integer)
    io.offset = pos
    io
end

function Base.seekstart(io::DummyRemoteResourceIO)
    seek(io, 0)
end

function Base.seekend(io::DummyRemoteResourceIO)
    seek(io, resc_length(io))
end

function Base.eof(io::DummyRemoteResourceIO)
    position(io) >= resc_length(io.resource)
end

function Base.bytesavailable(io::DummyRemoteResourceIO)
    return 0
end

function Base.isopen(io::DummyRemoteResourceIO)
    true
end

function Base.close(io::DummyRemoteResourceIO)
    nothing
end

function resc_length(x::DummyRemoteResourceIO)
    resc_length(x.resource)
end

function resc_fetch(x::DummyRemoteResourceIO, range::UnitRange{<:Integer})
    ### TODO: add stats
    resc_fetch(x.resource, range)
end

function TranscodingStreams.readdata!(input::DummyRemoteResourceIO, output::TranscodingStreams.Buffer)
    ntoread::Int = min(TranscodingStreams.marginsize(output), resc_length(input) - position(input))
    ntoread <= 0 && return 0
    range_start = position(input) + 1
    range_stop = position(input) + ntoread
    data::Vector{UInt8} = resc_fetch(input, range_start:range_stop)
    @assert length(data) == ntoread
    GC.@preserve data output unsafe_copyto!(TranscodingStreams.marginptr(output), pointer(data), ntoread)
    TranscodingStreams.supplied!(output, ntoread)
    input.offset += ntoread
    return ntoread
end


### The complete, buffered IO.

mutable struct RemoteResourceIO{T} <: IO
    dummy_io::DummyRemoteResourceIO{T}
    stream::TranscodingStreams.NoopStream{DummyRemoteResourceIO{T}}
    function RemoteResourceIO(resource::RemoteResource; bufsize=1*1024*1024)
        dummy_io = DummyRemoteResourceIO(resource)
        stream = TranscodingStreams.TranscodingStream(TranscodingStreams.Noop(), dummy_io; bufsize = bufsize)
        TranscodingStreams.NoopStream(dummy_io)
        new{typeof(resource)}(dummy_io, stream)
    end
    function RemoteResourceIO{T}(resource::T; kwargs...) where {T}
        RemoteResourceIO(resource; kwargs...)
    end
end

function Base.parent(x::RemoteResourceIO) x.stream end

function Base.position(x::RemoteResourceIO) position(parent(x)) end
function Base.read(x::RemoteResourceIO) read(parent(x)) end
function Base.read(x::RemoteResourceIO, nb::Integer) read(parent(x), nb) end
#function Base.read(x::RemoteResourceIO, T::Type) read(parent(x), T) end
function Base.read(x::RemoteResourceIO, T::Type{UInt8}) read(parent(x), T) end
function Base.read(x::RemoteResourceIO, T::Type{Char}) read(parent(x), T) end
function Base.seek(x::RemoteResourceIO, args...; kwargs...) seek(parent(x), args...; kwargs...) end
function Base.seekstart(x::RemoteResourceIO, args...; kwargs...) seekstart(parent(x), args...; kwargs...) end
function Base.seekend(x::RemoteResourceIO, args...; kwargs...) seekend(parent(x), args...; kwargs...) end
function Base.bytesavailable(x::RemoteResourceIO) bytesavailable(parent(x)) end
function Base.eof(x::RemoteResourceIO) eof(parent(x)) end
function Base.peek(x::RemoteResourceIO, args...; kwargs...) peek(parent(x), args...; kwargs...) end
function Base.isopen(x::RemoteResourceIO) isopen(parent(x)) end
function Base.close(x::RemoteResourceIO) close(parent(x)) end

include("http.jl")
include("gcs.jl")

end
