local tcp = ngx.socket.tcp
local udp = ngx.socket.udp
local timer_at = ngx.timer.at

local retry_send, retry_connect, max_retry_times = 0, 0, 3
local retry_interval = 100 --0.1s
local connect_timeout = 1000 --1s
local logger_initted
local host, port
local flush_limit, drop_limit = 1024 * 4, 1024 * 1024 * 1024
local flushing
local pool_size = 100
local sock_type = "tcp"
local last_error
local send_buffer = ""



local succ, new_tab = pcall(require, "table.new")
if not succ then
    new_tab = function() return {} end
end

local logger_buffer  = new_tab(20000, 0)
local logger_index = 0
local cur_buffer_length = 0


local function write_error(err)
    last_error = err 
end


local function flush_lock()
    if not flushing then
        ngx.log(ngx.DEBUG, "accquire locks")
        flushing = true
        return true
    end

    return false
end


local function flush_unlock()
    flushing = false
end

local function fill_buffer(msg, msg_len)
    logger_index = logger_index + 1
    logger_buffer[logger_index] = msg
    cur_buffer_length = cur_buffer_length + msg_len
    return cur_buffer_length
end


local function prepare_stream_buffer()
    local packet = table.concat(logger_buffer, "", 1, logger_index)
    send_buffer = send_buffer .. packet
    logger_index = 0
    --TODO
    --for avoding logger_buffer memory leak, create new buffer
end


local function do_connect()
    local ok, sock, err
    if sock_type == "udp" then
        sock, err = udp()
    elseif sock_type == "tcp" then
        sock, err = tcp()
    end

    if not sock then
        return nil, err
    end

    sock:settimeout(connect_timeout)

    if host and port then
        if sock_type == "udp" then
            ok, err = sock:setpeername(host, port)
        elseif sock_type == "tcp" then
            ok, err = sock:connect(host, port)
        end
    end

    if not ok then
        ngx.log(ngx.INFO, "failed to connect: ", err)
        return nil, err
    end
            
    return sock
end


local function _connect()
    local sock, err
    retry_connect = 0

    while retry_connect < max_retry_times do
        sock, err = do_connect()
        if sock then
            break
        end

        ngx.sleep(retry_interval / 1000)
        retry_connect = retry_connect + 1
    end
    if not sock then
        return nil, err
    end

    return sock
end


local function do_flush()
    local sock, err, bytes

    sock, err = _connect()
    if not sock then
        return nil, err
    end

    bytes, err = sock:send(send_buffer)
    if not bytes then
        ngx.log(ngx.INFO, "failed to send buffer: ", err)
        return nil, err
    end

    if sock_type ~= "udp" then
        sock:setkeepalive(0, pool_size)
    end

    return bytes
end    


local function _flush()
    if not flush_lock() then
        ngx.log(ngx.DEBUG, "previouse flush not finish yet")
        return true
    end

    local bytes, sock, err
    retry_send = 0
    while retry_send < max_retry_times do
        if logger_index > 0 then
            prepare_stream_buffer()
        end

        bytes, err = do_flush()
        if bytes then
            break
        end

        ngx.sleep(retry_interval / 1000)
        retry_send = retry_send + 1
    end

    flush_unlock()

    if not bytes then
        local err_msg = "failed to resend after " .. max_retry_times
        write_error(err_msg)
        return nil, err_msg
    end

    cur_buffer_length = cur_buffer_length - #send_buffer
    send_buffer = ""

    return bytes
end


local function flush_buffer()
    local ok, err = timer_at(0, _flush)
    if not ok then
        write_error(err)
        return nil, err
    end
end




local _M = new_tab(0, 5)
_M._VERSION = "0.0.1"


function _M.log(msg)
    if type(msg) ~= "string" then
        msg = tostring(msg)
    end

    local msg_len = #msg
    local bytes, err

    if msg_len + cur_buffer_length < flush_limit then
        bytes = msg_len
        fill_buffer(msg, msg_len)
    elseif msg_len + cur_buffer_length < drop_limit then
        bytes = msg_len
        fill_buffer(msg, msg_len) 
        flush_buffer()
    else
        flush_buffer()
        bytes = 0
    end

    if last_error then
        local err = last_error
        last_error = nil
        return bytes, err
    end

    return bytes
end

function _M.init(config) 
    if type(config) ~= "table" then
        return nil, "user config should be table type"
    end

    for k, v in pairs(config) do
        if k == "host" then
            host = v
        end

        if k == "port" then
            port = v 
        end

        if k == "flush_limit" then
            flush_limit = v
        end

        if k == "sock_type" then
            sock_type = v
        end

        if k == "drop_limit" then
            drop_limit = v 
        end
    end

    if not (host and port) then
        return nil, "need host and port in config"
    end

    if flush_limit > drop_limit then
        return nil, "flush_limit should < drop limit"
    end

    logger_initted = true

    return logger_initted
end


function _M.initted()
    return logger_initted
end

function _M.cur_buffer_len()
    return cur_buffer_length
end



return _M






