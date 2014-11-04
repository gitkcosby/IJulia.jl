import Base.show

export Msg, msg_pub, msg_reply, send_status, send_ipython, Message, MsgV5

# IPython message structure

abstract Message

type Msg <: Message
    idents::Vector{String}
    header::Dict
    content::Dict
    parent_header::Dict
    metadata::Dict
    function Msg(idents, header::Dict, content::Dict,
                 parent_header=Dict{String,Any}(), metadata=Dict{String,Any}())
        new(idents,header,content,parent_header,metadata)
    end
end

type MsgV5 <: Message
    idents::Vector{String}
    header::Dict
    content::Dict
    parent_header::Dict
    metadata::Dict
    function MsgV5(idents, header::Dict, content::Dict,
                 parent_header=Dict{String,Any}(), metadata=Dict{String,Any}())
        new(idents,header,content,parent_header,metadata)
    end
end


# PUB/broadcast messages use the msg_type as the ident, except for
# stream messages which use the stream name (e.g. "stdout").
# [According to minrk, "this isn't well defined, or even really part
# of the spec yet" and is in practice currently ignored since "all
# subscribers currently subscribe to all topics".]
msg_pub(m::Message, msg_type, content, metadata=Dict{String,Any}()) =
  Msg([ msg_type == "stream" ? content["name"] : msg_type ], 
      @compat(Dict("msg_id" => uuid4(),
                   "username" => m.header["username"],
                   "session" => m.header["session"],
                   "msg_type" => msg_type)),
      content, m.header, metadata)

msg_pub(m::MsgV5, msg_type, content, metadata=Dict{String,Any}()) =
  Msg([ msg_type == "stream" ? content["name"] : msg_type ], 
      @compat(Dict("msg_id" => uuid4(),
                   "username" => m.header["username"],
                   "session" => m.header["session"],
                   "msg_type" => msg_type,
                   "version" => "5.0")),
      content, m.header, metadata)

msg_reply(m::Message, msg_type, content, metadata=Dict{String,Any}()) =
  Msg(m.idents, 
      @compat(Dict("msg_id" => uuid4(),
                   "username" => m.header["username"],
                   "session" => m.header["session"],
                   "msg_type" => msg_type)),
      content, m.header, metadata)


msg_reply(m::MsgV5, msg_type, content, metadata=Dict{String,Any}()) =
  MsgV5(m.idents, 
      @compat(Dict("msg_id" => uuid4(),
                   "username" => m.header["username"],
                   "session" => m.header["session"],
                   "msg_type" => msg_type,
                   "version" => "5.0")),
      content, m.header, metadata)



function show(io::IO, msg::Message)
    print(io, "IPython Msg [ idents ")
    print_joined(io, msg.idents, ", ")
    print(io, " ] {\n  header = $(msg.header),\n  metadata = $(msg.metadata),\n  content = $(msg.content)\n}")
end

function send_ipython(socket, m::Message)
    @vprintln("SENDING $m")
    for i in m.idents
        send(socket, i, SNDMORE)
    end
    send(socket, "<IDS|MSG>", SNDMORE)
    header = json(m.header)
    parent_header = json(m.parent_header)
    metadata = json(m.metadata)
    content = json(m.content)
    send(socket, hmac(header, parent_header, metadata, content), SNDMORE)
    send(socket, header, SNDMORE)
    send(socket, parent_header, SNDMORE)
    send(socket, metadata, SNDMORE)
    send(socket, content)
end

function recv_ipython(socket)
    msg = recv(socket)
    idents = String[]
    s = bytestring(msg)
    @vprintln("got msg part $s")
    while s != "<IDS|MSG>"
        push!(idents, s)
        msg = recv(socket)
        s = bytestring(msg)
        @vprintln("got msg part $s")
    end
    signature = bytestring(recv(socket))
    request = Dict{String,Any}()
    header = bytestring(recv(socket))
    parent_header = bytestring(recv(socket))
    metadata = bytestring(recv(socket))
    content = bytestring(recv(socket))
    if signature != hmac(header, parent_header, metadata, content)
        error("Invalid HMAC signature") # What should we do here?
    end
    h = JSON.parse(header)
    version = get(h,"version","4.0")
    if version == "4.0"
        m = Msg(idents, h, JSON.parse(content), JSON.parse(parent_header), JSON.parse(metadata))
    elseif version == "5.0"
        m = MsgV5(idents, h, JSON.parse(content), JSON.parse(parent_header), JSON.parse(metadata))
    end
        
    @vprintln("RECEIVED $m")
    return m
end

function send_status(state::String, parent_header=nothing)
    if parent_header == nothing
        msg = Msg(
                  [ "status" ],
                  @compat(Dict("msg_id" => uuid4(),
                               "username" => "jlkernel",
                               "session" => execute_msg.header["session"],
                               "msg_type" => "status")),
                  @compat(Dict("execution_state" => state))
        )
    else
        msg = Msg(
                  [ "status" ],
                  @compat(Dict("msg_id" => uuid4(),
                               "username" => "jlkernel",
                               "session" => execute_msg.header["session"],
                               "msg_type" => "status")),
                  @compat(Dict("execution_state" => state)),
                  parent_header
        )
    end
    send_ipython(publish, msg)
end
