dofile("urlcode.lua")
local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
JSON = (loadfile "JSON.lua")()

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

if urlparse == nil or http == nil then
  io.stdout:write("socket not corrently installed.\n")
  io.stdout:flush()
  abortgrab = true
end

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local discovered_images = {}
local bad_items = {}
local ids = {}

local retry_url = false

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

abort_item = function(item)
  abortgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  local value = string.match(url, "^https?://bcy%.net/item/detail/([0-9]+)$")
  local type_ = "item"
  if not value then
    value = string.match(url, "^https?://bcy%.net/u/([0-9]+)$")
    type_ = "user"
  end
  --[[if not value then
    value = string.match(url, "^https?://bcy%.net/collection/([0-9]+)$")
    type_ = "collection"
  end
  if not value then
    value = string.match(url, "^https?://bcy%.net/huodong/([0-9]+)$")
    type_ = "huodong"
  end
  if not value then
    value = string.match(url, "^https?://bcy%.net/group/list/([0-9]+)$")
    type_ = "group"
  end]]
  if value then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    item_type = found["type"]
    item_value = found["value"]
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      ids[item_value] = true
      abortgrab = false
      initial_allowed = false
      tries = 0
      retry_url = false
      allow_video = false
      webpage_404 = false
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url]
    or string.match(url, "^https?://bcy%.net/s/") then
    return true
  end

  if not string.match(url, "^https?://.")
    or string.match(url, "/apiv3/user/favor") then
    return false
  end

  if select(2, string.gsub(url, "/", "")) < 3 then
    url = url .. "/"
  end

  if string.match(url, "^https?://[^/]+bcyimg%.com/") then
    discover_item(discovered_images, url)
    return false
  end

  for pattern, type_ in pairs({
    ["^https?://bcy%.net/item/detail/([0-9]+)"]="item",
    ["^https?://bcy%.net/u/([0-9]+)"]="user",
    ["^https?://bcy%.net/collection/([0-9]+)"]="collection",
    ["^https?://bcy%.net/item/set/detail/([0-9]+)"]="collection",
    ["^https?://bcy%.net/huodong/([0-9]+)"]="huodong",
    ["^https?://bcy%.net/group/([0-9]+)"]="group",
    ["^https?://bcy%.net/circle/index/([0-9]+)"]="tag",
    ["^https?://bcy%.net/tag/([0-9]+)"]="tag",
    ["^https?://bcy%.net/video/list/([0-9]+)"]="videos"
  }) do
    local match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        return false
      end
    end
  end

  for _, pattern in pairs({
    "([0-9a-zA-Z]+)"
  }) do
    for s in string.gmatch(string.match(url, "^https?://[^/]+(/.*)"), pattern) do
      if ids[s] then
        return true
      end
    end
  end

  if not string.match(url, "^https?://bcy%.net/") then
    discover_item(discovered_outlinks, url)
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  --[[if allowed(url, parent["url"]) and not processed(url) then
    addedtolist[url] = true
    return true
  end]]

  return false
end

local function get_ssr_data(html)
  local ssr_data = string.match(html, 'window%.__ssr_data%s*=%s*JSON%.parse%(("{.-}")%);')
  if ssr_data then
    local json = JSON:decode("[" .. ssr_data .. "]")
    return JSON:decode(json[1])
  end
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return unicode_codepoint_as_utf8(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function fix_case(newurl)
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    newurl = decode_codepoint(newurl)
    --newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      if string.match(url_, "^https?://bcy%.net/apiv3/") then
        table.insert(urls, {
          url=url_,
          headers={
            ["x-requested-with"]="XMLHttpRequest"
          }
        })
      else
        table.insert(urls, { url=url_ })
      end
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function post_request(newurl, data)
    local data = JSON:encode(data)
    local check_s = newurl .. data
    if not processed(check_s) then
      table.insert(urls, {
        url=newurl,
        method="POST",
        body_data=data,
        headers={
          ["Accept"]="application/json",
          ["Content-Type"]="application/json",
          ["X-Requested-With"]="XMLHttpRequest"
        }
      })
      addedtolist(check_s)
    end
  end

  local function checknewurl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  local inner_url = string.match(url, "..-(https?://.+)$")
  if inner_url then
    check(inner_url)
  end

  local function check_new_params(newurl, param, value)
    if string.match(newurl, "[%?&]" .. param .. "=") then
      newurl = string.gsub(newurl, "([%?&]" .. param .. "=)[^%?&;]+", "%1" .. value)
    else
      if string.match(newurl, "%?") then
        newurl = newurl .. "&"
      else
        newurl = newurl .. "?"
      end
      newurl = newurl .. param .. "=" .. value
    end
    check(newurl)
  end

  local function queue_next(url, param, default)
    if not default then
      default = 1
    end
    local num = string.match(url, "[%?&]" .. param .. "=([0-9]+)")
    if num then
      num = tonumber(num) + 1
    else
      num = default
    end
    num = tostring(num)
    check_new_params(url, param, num)
  end

  local function count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end
    return count
  end

  local function discover_from_json(data)
    for k, v in pairs(data) do
      local type_v = type(v)
      if type_v == "table" then
        discover_from_json(v)
      elseif type(k) == "string" then
        v = tostring(v)
        if k == "tag_id"
          or k == "circle_id" then
          discover_item(discovered_items, "tag:" .. v)
        elseif k == "collection_id" then
          discover_item(discovered_items, "collection:" .. v)
        elseif k == "item_id" then
          discover_item(discovered_items, "item:" .. v)
        elseif k == "event_id" then
          discover_item(discovered_items, "event:" .. v)
        elseif k == "cid" then
          discover_item(discovered_items, "videos:" .. v)
        elseif k == "group_id"
          or k == "gid" then
          discover_item(discovered_items, "group:" .. v)
        elseif string.match(k, "uid$") then
          discover_item(discovered_items, "user:" .. v)
        end
      end
    end
  end

  local function get_last(json, keys)
    local result = nil
    if not json then
      return result
    end
    if type(keys) == "string" then
      keys = {keys}
    end
    for _, data in pairs(json) do
      for _, key in pairs(keys) do
        data = data[key]
      end
      result = data
    end
    return result
  end

  if allowed(url)
    and status_code < 300 then
    html = read_file(file)
    local ssr = get_ssr_data(html)
    if ssr then
      discover_from_json(ssr)
      html = html .. flatten_json(ssr)
    end
    local json = nil
    if string.match(url, "^https?://bcy%.net/apiv3/") then
      json = JSON:decode(html)
      discover_from_json(json)
      html = html .. flatten_json(json)
    end

    if string.match(url, "[%?&]since=") then
      check_new_params(url, "since", "0")
    end

    if string.match(url, "[%?&]p=") then
      check_new_params(url, "p", "1")
    end

    -- user
    if string.match(url, "^https?://bcy%.net/u/[0-9]+$") then
      local base_url = "https://bcy.net/apiv3/user/selfPosts?uid=" .. item_value
      check(base_url)
      check(base_url .. "&since=0")
      if ssr["page"]["since"] then
        check(base_url .. "&since=" .. ssr["page"]["since"])
      end
      for _, s in pairs({
        "like",
        "collection",
        "circle",
        "follower",
        "following",
        -- other style
        "post/",
        "post/?p=1"
      }) do
        check("https://bcy.net/u/" .. item_value .. "/" .. s)
      end
      check("https://bcy.net/apiv3/user/follow-list?uid=" .. item_value .. "&follow_type=0")
      check("https://bcy.net/apiv3/user/follow-list?uid=" .. item_value .. "&page=1&follow_type=0")
      check("https://bcy.net/apiv3/user/follow-list?uid=" .. item_value .. "&follow_type=1")
      check("https://bcy.net/apiv3/user/follow-list?uid=" .. item_value .. "&page=1&follow_type=1")
      check("https://bcy.net/apiv3/collection/getMyCollectionList?uid=" .. item_value .. "&since=" .. tostring(os.time(os.date("!*t"))))
      for _, s in pairs({
        "all",
        "note",
        "article",
        "ganswer",
        "video"
      }) do
        check("https://bcy.net/u/" .. item_value .. "?filter=" .. s)
        if s ~= "all" then
          check("https://bcy.net/u/" .. item_value .. "/post/" .. s)
          check("https://bcy.net/u/" .. item_value .. "/post/" .. s .. "?p=1")
        end
        -- skipping the POST requests unfortunately
        --[[post_request(
          "https://bcy.net/apiv3/user/post",
          {
            ["uid"]=10444,
            ["ptype"]=s,
            ["page"]=1,
            ["mid"]="0",
            ["_csrf_token"]="TODO"
          }
        )]]
      end
    end
    if string.match(url, "^https?://bcy%.net/u/[^/]+/like") then
      check("https://bcy.net/apiv3/user/favor?uid=" .. item_value .. "&ptype=like&mid=" .. ssr["user"]["uid"] .. "&since=" .. tostring(ssr["page"]["since"]) .. "&size=35")
    end
    if string.match(url, "^https?://bcy%.net/u/[^/]+/post") then
      if count(ssr["post_data"]["list"]) > 0 then
        queue_next(url, "p", 1)
      end
    end
    if string.match(url, "^https?://bcy%.net/u/[^/]+/collection") then
      if ssr["page"]["since"] then
        check("https://bcy.net/apiv3/collection/getMyCollectionList?uid=" .. item_value .. "&since=" .. tostring(ssr["page"]["since"]))
      end
    end
    if string.match(url, "^https?://bcy%.net/apiv3/collection/getMyCollectionList") then
      local last_since = get_last(json["data"]["collections"], {"collection", "since"})
      if last_since then
        check_new_params(url, "since", last_since)
      end
    end
    if string.match(url, "^https?://bcy%.net/apiv3/user/selfPosts") then
      local last_since = get_last(json["data"]["items"], "since")
      if last_since then
        check_new_params(url, "since", last_since)
      end
    end
    if string.match(url, "^https?://bcy%.net/apiv3/user/favor") then
      local last_since = get_last(json["data"]["list"], "since")
      if last_since then
        check_new_params(url, "since", last_since)
      end
    end
    if string.match(url, "^https?://bcy%.net/apiv3/user/follow%-list") then
      if count(json["data"]["user_follow_info"]) > 0 then
        queue_next(url, "page", 1)
      end
    end

    -- item
    if string.match(url, "^https?://bcy%.net/item/detail/") then
      -- not getting video yet
      for _, sort in pairs({"hot", "time"}) do
        check("https://bcy.net/apiv3/cmt/reply/list?page=1&item_id=" .. item_value .. "&limit=15&sort=" .. sort)
        check("https://bcy.net/apiv3/cmt/reply/list?item_id=" .. item_value .. "&limit=15&sort=" .. sort)
      end
    end
    if string.match(url, "^https?://bcy%.net/apiv3/cmt/reply/list") then
      if count(json["data"]["data"]) > 0 then
        queue_next(url, "page", 1)
      end
    end

    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    html = string.gsub(html, "&gt;", ">")
    html = string.gsub(html, "&lt;", "<")
    for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
      checknewurl(newurl)
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  local html = nil
  if not item_name then
    error("No item name found.")
  end
  if string.match(url["url"], "^https?://bcy%.net/item/detail/") then
    if not html then
      html = read_file(http_stat["local_file"])
    end
    json = get_ssr_data(html)
    if json["detail"]["post_data"]["type"] == "video" then
      print("Skipping videos for now.")
      abort_item()
      return false
    end
  end
  if string.match(url["url"], "^https?://bcy%.net/apiv3/") then
    if not html then
      html = read_file(http_stat["local_file"])
    end
    if not string.match(html, "^%s*{")
      or not string.match(html, "}%s*$") then
      print("Did not get JSON data.")
      retry_url = true
      return false
    end
    local json = JSON:decode(html)
    if json["code"] ~= 0
      or json["msg"] ~= "" then
      print("Bad code in JSON.")
      retry_url = true
      return false
    end
  elseif string.match(url["url"], "^https?://bcy%.net/u/")
    or string.match(url["url"], "^https?://bcy%.net/item/") then
    if not html then
      html = read_file(http_stat["local_file"])
    end
    if not string.match(html, "</html>") then
      print("Bad HTML.")
      retry_url = true
      return false
    end
  end
  if http_stat["statcode"] ~= 200 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end
  
  if status_code < 400 then
    downloaded[url["url"]] = true
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    if tries > 5 then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 10
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and JSON:decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["banciyuan-nk4dhjt8pcqhn56z"]=discovered_items,
    ["urls-rt62d2tgo2ctf2ui"]=discovered_outlinks,
    ["banciyuan-images-mz48dpc97ktci5t0"]=discovered_images
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


