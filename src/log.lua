local logger = require "lua-resty-logger-push.lib.logger.socket"

if not logger.initted() then
    local ok, err = logger.init {
        host = "127.0.0.1",
        port = 8881,
        flush_limit = 1024,
    }

    if not ok then
        ngx.log(ngx.ERR, "faild to init logger: ", err)
        return
    end
end


local bytes, err = logger.log(ngx.var.request_uri)
if err then
    ngx.log(ngx.ERR, "failed to logg: ", err)
    return
end


--ngx.log(ngx.INFO, "success push"," cur buffer length: ", 
 --       logger.cur_buffer_len())
