# HttpIO

This package provides an IO type for reading large remote files that served through a server.
It will convert IO requests to http requests and use 'Range' header to request partial
content. Think of it as read-only memory map for remote resources.

Additionally, it provides support for files served with google cloud storage.
