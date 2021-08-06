using HttpIO
using Test

@testset "http io works" begin
    io = HttpFileIO("https://hgdownload.soe.ucsc.edu/goldenPath/hg38/bigZips/xenoMrna.fa.gz")

    # seekend
    seekend(io)
    @test position(io) == 7144575001
    @test read(io) == UInt8[]

    # seekstart
    seekstart(io)
    @test position(io) == 0
end
