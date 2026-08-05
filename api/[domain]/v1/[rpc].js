var __create = Object.create;
var __defProp = Object.defineProperty;
var __getOwnPropDesc = Object.getOwnPropertyDescriptor;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __getProtoOf = Object.getPrototypeOf;
var __hasOwnProp = Object.prototype.hasOwnProperty;
var __require = /* @__PURE__ */ ((x) => typeof require !== "undefined" ? require : typeof Proxy !== "undefined" ? new Proxy(x, {
  get: (a, b) => (typeof require !== "undefined" ? require : a)[b]
}) : x)(function(x) {
  if (typeof require !== "undefined") return require.apply(this, arguments);
  throw Error('Dynamic require of "' + x + '" is not supported');
});
var __commonJS = (cb, mod) => function __require2() {
  return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
};
var __copyProps = (to, from, except, desc) => {
  if (from && typeof from === "object" || typeof from === "function") {
    for (let key of __getOwnPropNames(from))
      if (!__hasOwnProp.call(to, key) && key !== except)
        __defProp(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc(from, key)) || desc.enumerable });
  }
  return to;
};
var __toESM = (mod, isNodeMode, target) => (target = mod != null ? __create(__getProtoOf(mod)) : {}, __copyProps(
  // If the importer is in node compatibility mode or this is not an ESM
  // file that has been converted to a CommonJS file using a Babel-
  // compatible transform (i.e. "__esModule" has not been set), then set
  // "default" to the CommonJS "module.exports" for node compatibility.
  isNodeMode || !mod || !mod.__esModule ? __defProp(target, "default", { value: mod, enumerable: true }) : target,
  mod
));

// node_modules/@upstash/core-analytics/dist/index.js
var require_dist = __commonJS({
  "node_modules/@upstash/core-analytics/dist/index.js"(exports2, module) {
    "use strict";
    var g = Object.defineProperty;
    var k = Object.getOwnPropertyDescriptor;
    var _ = Object.getOwnPropertyNames;
    var y = Object.prototype.hasOwnProperty;
    var w = (l, e) => {
      for (var t in e) g(l, t, { get: e[t], enumerable: true });
    };
    var A = (l, e, t, i) => {
      if (e && typeof e == "object" || typeof e == "function") for (let s of _(e)) !y.call(l, s) && s !== t && g(l, s, { get: () => e[s], enumerable: !(i = k(e, s)) || i.enumerable });
      return l;
    };
    var x = (l) => A(g({}, "__esModule", { value: true }), l);
    var S = {};
    w(S, { Analytics: () => b });
    module.exports = x(S);
    var p = `
local key = KEYS[1]
local field = ARGV[1]

local data = redis.call("ZRANGE", key, 0, -1, "WITHSCORES")
local count = {}

for i = 1, #data, 2 do
  local json_str = data[i]
  local score = tonumber(data[i + 1])
  local obj = cjson.decode(json_str)

  local fieldValue = obj[field]

  if count[fieldValue] == nil then
    count[fieldValue] = score
  else
    count[fieldValue] = count[fieldValue] + score
  end
end

local result = {}
for k, v in pairs(count) do
  table.insert(result, {k, v})
end

return result
`;
    var f = `
local prefix = KEYS[1]
local first_timestamp = tonumber(ARGV[1]) -- First timestamp to check
local increment = tonumber(ARGV[2])       -- Increment between each timestamp
local num_timestamps = tonumber(ARGV[3])  -- Number of timestampts to check (24 for a day and 24 * 7 for a week)
local num_elements = tonumber(ARGV[4])    -- Number of elements to fetch in each category
local check_at_most = tonumber(ARGV[5])   -- Number of elements to check at most.

local keys = {}
for i = 1, num_timestamps do
  local timestamp = first_timestamp - (i - 1) * increment
  table.insert(keys, prefix .. ":" .. timestamp)
end

-- get the union of the groups
local zunion_params = {"ZUNION", num_timestamps, unpack(keys)}
table.insert(zunion_params, "WITHSCORES")
local result = redis.call(unpack(zunion_params))

-- select num_elements many items
local true_group = {}
local false_group = {}
local denied_group = {}
local true_count = 0
local false_count = 0
local denied_count = 0
local i = #result - 1

-- index to stop at after going through "checkAtMost" many items:
local cutoff_index = #result - 2 * check_at_most

-- iterate over the results
while (true_count + false_count + denied_count) < (num_elements * 3) and 1 <= i and i >= cutoff_index do
  local score = tonumber(result[i + 1])
  if score > 0 then
    local element = result[i]
    if string.find(element, "success\\":true") and true_count < num_elements then
      table.insert(true_group, {score, element})
      true_count = true_count + 1
    elseif string.find(element, "success\\":false") and false_count < num_elements then
      table.insert(false_group, {score, element})
      false_count = false_count + 1
    elseif string.find(element, "success\\":\\"denied") and denied_count < num_elements then
      table.insert(denied_group, {score, element})
      denied_count = denied_count + 1
    end
  end
  i = i - 2
end

return {true_group, false_group, denied_group}
`;
    var h = `
local prefix = KEYS[1]
local first_timestamp = tonumber(ARGV[1])
local increment = tonumber(ARGV[2])
local num_timestamps = tonumber(ARGV[3])

local keys = {}
for i = 1, num_timestamps do
  local timestamp = first_timestamp - (i - 1) * increment
  table.insert(keys, prefix .. ":" .. timestamp)
end

-- get the union of the groups
local zunion_params = {"ZUNION", num_timestamps, unpack(keys)}
table.insert(zunion_params, "WITHSCORES")
local result = redis.call(unpack(zunion_params))

return result
`;
    var b = class {
      redis;
      prefix;
      bucketSize;
      constructor(e) {
        this.redis = e.redis, this.prefix = e.prefix ?? "@upstash/analytics", this.bucketSize = this.parseWindow(e.window);
      }
      validateTableName(e) {
        if (!/^[a-zA-Z0-9_-]+$/.test(e)) throw new Error(`Invalid table name: ${e}. Table names can only contain letters, numbers, dashes and underscores.`);
      }
      parseWindow(e) {
        if (typeof e == "number") {
          if (e <= 0) throw new Error(`Invalid window: ${e}`);
          return e;
        }
        let t = /^(\d+)([smhd])$/;
        if (!t.test(e)) throw new Error(`Invalid window: ${e}`);
        let [, i, s] = e.match(t), n = parseInt(i);
        switch (s) {
          case "s":
            return n * 1e3;
          case "m":
            return n * 1e3 * 60;
          case "h":
            return n * 1e3 * 60 * 60;
          case "d":
            return n * 1e3 * 60 * 60 * 24;
          default:
            throw new Error(`Invalid window unit: ${s}`);
        }
      }
      getBucket(e) {
        let t = e ?? Date.now();
        return Math.floor(t / this.bucketSize) * this.bucketSize;
      }
      async ingest(e, ...t) {
        this.validateTableName(e), await Promise.all(t.map(async (i) => {
          let s = this.getBucket(i.time), n = [this.prefix, e, s].join(":");
          await this.redis.zincrby(n, 1, JSON.stringify({ ...i, time: void 0 }));
        }));
      }
      formatBucketAggregate(e, t, i) {
        let s = {};
        return e.forEach(([n, r]) => {
          t == "success" && (n = n === 1 ? "true" : n === null ? "false" : n), s[t] = s[t] || {}, s[t][(n ?? "null").toString()] = r;
        }), { time: i, ...s };
      }
      async aggregateBucket(e, t, i) {
        this.validateTableName(e);
        let s = this.getBucket(i), n = [this.prefix, e, s].join(":"), r = await this.redis.eval(p, [n], [t]);
        return this.formatBucketAggregate(r, t, s);
      }
      async aggregateBuckets(e, t, i, s) {
        this.validateTableName(e);
        let n = this.getBucket(s), r = [];
        for (let o = 0; o < i; o += 1) r.push(this.aggregateBucket(e, t, n)), n = n - this.bucketSize;
        return Promise.all(r);
      }
      async aggregateBucketsWithPipeline(e, t, i, s, n) {
        this.validateTableName(e), n = n ?? 48;
        let r = this.getBucket(s), o = [], c = this.redis.pipeline(), u = [];
        for (let a = 1; a <= i; a += 1) {
          let d = [this.prefix, e, r].join(":");
          c.eval(p, [d], [t]), o.push(r), r = r - this.bucketSize, (a % n == 0 || a == i) && (u.push(c.exec()), c = this.redis.pipeline());
        }
        return (await Promise.all(u)).flat().map((a, d) => this.formatBucketAggregate(a, t, o[d]));
      }
      async getAllowedBlocked(e, t, i) {
        this.validateTableName(e);
        let s = [this.prefix, e].join(":"), n = this.getBucket(i), r = await this.redis.eval(h, [s], [n, this.bucketSize, t]), o = {};
        for (let c = 0; c < r.length; c += 2) {
          let u = r[c], m = u.identifier, a = +r[c + 1];
          o[m] || (o[m] = { success: 0, blocked: 0 }), o[m][u.success ? "success" : "blocked"] = a;
        }
        return o;
      }
      async getMostAllowedBlocked(e, t, i, s, n) {
        this.validateTableName(e);
        let r = [this.prefix, e].join(":"), o = this.getBucket(s), c = n ?? i * 5, [u, m, a] = await this.redis.eval(f, [r], [o, this.bucketSize, t, i, c]);
        return { allowed: this.toDicts(u), ratelimited: this.toDicts(m), denied: this.toDicts(a) };
      }
      toDicts(e) {
        let t = [];
        for (let i = 0; i < e.length; i += 1) {
          let s = +e[i][0], n = e[i][1];
          t.push({ identifier: n.identifier, count: s });
        }
        return t;
      }
    };
  }
});

// node_modules/@upstash/ratelimit/dist/index.js
var require_dist2 = __commonJS({
  "node_modules/@upstash/ratelimit/dist/index.js"(exports2, module) {
    "use strict";
    var __defProp3 = Object.defineProperty;
    var __getOwnPropDesc2 = Object.getOwnPropertyDescriptor;
    var __getOwnPropNames2 = Object.getOwnPropertyNames;
    var __hasOwnProp2 = Object.prototype.hasOwnProperty;
    var __export2 = (target, all) => {
      for (var name in all)
        __defProp3(target, name, { get: all[name], enumerable: true });
    };
    var __copyProps2 = (to, from, except, desc) => {
      if (from && typeof from === "object" || typeof from === "function") {
        for (let key of __getOwnPropNames2(from))
          if (!__hasOwnProp2.call(to, key) && key !== except)
            __defProp3(to, key, { get: () => from[key], enumerable: !(desc = __getOwnPropDesc2(from, key)) || desc.enumerable });
      }
      return to;
    };
    var __toCommonJS = (mod) => __copyProps2(__defProp3({}, "__esModule", { value: true }), mod);
    var src_exports = {};
    __export2(src_exports, {
      Analytics: () => Analytics2,
      IpDenyList: () => ip_deny_list_exports,
      MultiRegionRatelimit: () => MultiRegionRatelimit,
      Ratelimit: () => RegionRatelimit
    });
    module.exports = __toCommonJS(src_exports);
    var import_core_analytics = require_dist();
    var Analytics2 = class {
      analytics;
      table = "events";
      constructor(config2) {
        this.analytics = new import_core_analytics.Analytics({
          // @ts-expect-error we need to fix the types in core-analytics, it should only require the methods it needs, not the whole sdk
          redis: config2.redis,
          window: "1h",
          prefix: config2.prefix ?? "@upstash/ratelimit",
          retention: "90d"
        });
      }
      /**
       * Try to extract the geo information from the request
       *
       * This handles Vercel's `req.geo` and  and Cloudflare's `request.cf` properties
       * @param req
       * @returns
       */
      extractGeo(req) {
        if (req.geo !== void 0) {
          return req.geo;
        }
        if (req.cf !== void 0) {
          return req.cf;
        }
        return {};
      }
      async record(event) {
        await this.analytics.ingest(this.table, event);
      }
      async series(filter, cutoff) {
        const timestampCount = Math.min(
          (this.analytics.getBucket(Date.now()) - this.analytics.getBucket(cutoff)) / (60 * 60 * 1e3),
          256
        );
        return this.analytics.aggregateBucketsWithPipeline(this.table, filter, timestampCount);
      }
      async getUsage(cutoff = 0) {
        const timestampCount = Math.min(
          (this.analytics.getBucket(Date.now()) - this.analytics.getBucket(cutoff)) / (60 * 60 * 1e3),
          256
        );
        const records = await this.analytics.getAllowedBlocked(this.table, timestampCount);
        return records;
      }
      async getUsageOverTime(timestampCount, groupby) {
        const result = await this.analytics.aggregateBucketsWithPipeline(this.table, groupby, timestampCount);
        return result;
      }
      async getMostAllowedBlocked(timestampCount, getTop, checkAtMost) {
        getTop = getTop ?? 5;
        const timestamp = void 0;
        return this.analytics.getMostAllowedBlocked(this.table, timestampCount, getTop, timestamp, checkAtMost);
      }
    };
    var Cache = class {
      /**
       * Stores identifier -> reset (in milliseconds)
       */
      cache;
      constructor(cache) {
        this.cache = cache;
      }
      isBlocked(identifier) {
        if (!this.cache.has(identifier)) {
          return { blocked: false, reset: 0 };
        }
        const reset = this.cache.get(identifier);
        if (reset < Date.now()) {
          this.cache.delete(identifier);
          return { blocked: false, reset: 0 };
        }
        return { blocked: true, reset };
      }
      blockUntil(identifier, reset) {
        this.cache.set(identifier, reset);
      }
      set(key, value) {
        this.cache.set(key, value);
      }
      get(key) {
        return this.cache.get(key) || null;
      }
      incr(key, incrementAmount = 1) {
        let value = this.cache.get(key) ?? 0;
        value += incrementAmount;
        this.cache.set(key, value);
        return value;
      }
      pop(key) {
        this.cache.delete(key);
      }
      empty() {
        this.cache.clear();
      }
      size() {
        return this.cache.size;
      }
    };
    var DYNAMIC_LIMIT_KEY_SUFFIX = ":dynamic:global";
    var DEFAULT_PREFIX = "@upstash/ratelimit";
    function ms(d) {
      const match = d.match(/^(\d+)\s?(ms|s|m|h|d)$/);
      if (!match) {
        throw new Error(`Unable to parse window size: ${d}`);
      }
      const time = Number.parseInt(match[1]);
      const unit = match[2];
      switch (unit) {
        case "ms": {
          return time;
        }
        case "s": {
          return time * 1e3;
        }
        case "m": {
          return time * 1e3 * 60;
        }
        case "h": {
          return time * 1e3 * 60 * 60;
        }
        case "d": {
          return time * 1e3 * 60 * 60 * 24;
        }
        default: {
          throw new Error(`Unable to parse window size: ${d}`);
        }
      }
    }
    var safeEval = async (ctx, script, keys, args) => {
      try {
        return await ctx.redis.evalsha(script.hash, keys, args);
      } catch (error) {
        if (`${error}`.includes("NOSCRIPT")) {
          return await ctx.redis.eval(script.script, keys, args);
        }
        throw error;
      }
    };
    var fixedWindowLimitScript = `
  local key           = KEYS[1]
  local dynamicLimitKey = KEYS[2]  -- optional: key for dynamic limit in redis
  local tokens        = tonumber(ARGV[1])  -- default limit
  local window        = ARGV[2]
  local incrementBy   = ARGV[3] -- increment rate per request at a given value, default is 1

  -- Check for dynamic limit
  local effectiveLimit = tokens
  if dynamicLimitKey ~= "" then
    local dynamicLimit = redis.call("GET", dynamicLimitKey)
    if dynamicLimit then
      effectiveLimit = tonumber(dynamicLimit)
    end
  end

  local r = redis.call("INCRBY", key, incrementBy)
  if r == tonumber(incrementBy) then
  -- The first time this key is set, the value will be equal to incrementBy.
  -- So we only need the expire command once
  redis.call("PEXPIRE", key, window)
  end

  return {r, effectiveLimit}
`;
    var fixedWindowRemainingTokensScript = `
  local key = KEYS[1]
  local dynamicLimitKey = KEYS[2]  -- optional: key for dynamic limit in redis
  local tokens = tonumber(ARGV[1])  -- default limit

  -- Check for dynamic limit
  local effectiveLimit = tokens
  if dynamicLimitKey ~= "" then
    local dynamicLimit = redis.call("GET", dynamicLimitKey)
    if dynamicLimit then
      effectiveLimit = tonumber(dynamicLimit)
    end
  end

  local value = redis.call('GET', key)
  local usedTokens = 0
  if value then
    usedTokens = tonumber(value)
  end
  
  return {effectiveLimit - usedTokens, effectiveLimit}
`;
    var slidingWindowLimitScript = `
  local currentKey  = KEYS[1]           -- identifier including prefixes
  local previousKey = KEYS[2]           -- key of the previous bucket
  local dynamicLimitKey = KEYS[3]       -- optional: key for dynamic limit in redis
  local tokens      = tonumber(ARGV[1]) -- default tokens per window
  local now         = ARGV[2]           -- current timestamp in milliseconds
  local window      = ARGV[3]           -- interval in milliseconds
  local incrementBy = tonumber(ARGV[4]) -- increment rate per request at a given value, default is 1

  -- Check for dynamic limit
  local effectiveLimit = tokens
  if dynamicLimitKey ~= "" then
    local dynamicLimit = redis.call("GET", dynamicLimitKey)
    if dynamicLimit then
      effectiveLimit = tonumber(dynamicLimit)
    end
  end

  local requestsInCurrentWindow = redis.call("GET", currentKey)
  if requestsInCurrentWindow == false then
    requestsInCurrentWindow = 0
  end

  local requestsInPreviousWindow = redis.call("GET", previousKey)
  if requestsInPreviousWindow == false then
    requestsInPreviousWindow = 0
  end
  local percentageInCurrent = ( now % window ) / window
  -- weighted requests to consider from the previous window
  requestsInPreviousWindow = math.floor(( 1 - percentageInCurrent ) * requestsInPreviousWindow)

  -- Only check limit if not refunding (negative rate)
  if incrementBy > 0 and requestsInPreviousWindow + requestsInCurrentWindow >= effectiveLimit then
    return {-1, effectiveLimit}
  end

  local newValue = redis.call("INCRBY", currentKey, incrementBy)
  if newValue == incrementBy then
    -- The first time this key is set, the value will be equal to incrementBy.
    -- So we only need the expire command once
    redis.call("PEXPIRE", currentKey, window * 2 + 1000) -- Enough time to overlap with a new window + 1 second
  end
  return {effectiveLimit - ( newValue + requestsInPreviousWindow ), effectiveLimit}
`;
    var slidingWindowRemainingTokensScript = `
  local currentKey  = KEYS[1]           -- identifier including prefixes
  local previousKey = KEYS[2]           -- key of the previous bucket
  local dynamicLimitKey = KEYS[3]       -- optional: key for dynamic limit in redis
  local tokens      = tonumber(ARGV[1]) -- default tokens per window
  local now         = ARGV[2]           -- current timestamp in milliseconds
  local window      = ARGV[3]           -- interval in milliseconds

  -- Check for dynamic limit
  local effectiveLimit = tokens
  if dynamicLimitKey ~= "" then
    local dynamicLimit = redis.call("GET", dynamicLimitKey)
    if dynamicLimit then
      effectiveLimit = tonumber(dynamicLimit)
    end
  end

  local requestsInCurrentWindow = redis.call("GET", currentKey)
  if requestsInCurrentWindow == false then
    requestsInCurrentWindow = 0
  end

  local requestsInPreviousWindow = redis.call("GET", previousKey)
  if requestsInPreviousWindow == false then
    requestsInPreviousWindow = 0
  end

  local percentageInCurrent = ( now % window ) / window
  -- weighted requests to consider from the previous window
  requestsInPreviousWindow = math.floor(( 1 - percentageInCurrent ) * requestsInPreviousWindow)

  local usedTokens = requestsInPreviousWindow + requestsInCurrentWindow
  return {effectiveLimit - usedTokens, effectiveLimit}
`;
    var tokenBucketLimitScript = `
  local key         = KEYS[1]           -- identifier including prefixes
  local dynamicLimitKey = KEYS[2]       -- optional: key for dynamic limit in redis
  local maxTokens   = tonumber(ARGV[1]) -- default maximum number of tokens
  local interval    = tonumber(ARGV[2]) -- size of the window in milliseconds
  local refillRate  = tonumber(ARGV[3]) -- how many tokens are refilled after each interval
  local now         = tonumber(ARGV[4]) -- current timestamp in milliseconds
  local incrementBy = tonumber(ARGV[5]) -- how many tokens to consume, default is 1

  -- Check for dynamic limit
  local effectiveLimit = maxTokens
  if dynamicLimitKey ~= "" then
    local dynamicLimit = redis.call("GET", dynamicLimitKey)
    if dynamicLimit then
      effectiveLimit = tonumber(dynamicLimit)
    end
  end
        
  local bucket = redis.call("HMGET", key, "refilledAt", "tokens")
        
  local refilledAt
  local tokens

  if bucket[1] == false then
    refilledAt = now
    tokens = effectiveLimit
  else
    refilledAt = tonumber(bucket[1])
    tokens = tonumber(bucket[2])
  end
        
  if now >= refilledAt + interval then
    local numRefills = math.floor((now - refilledAt) / interval)
    tokens = math.min(effectiveLimit, tokens + numRefills * refillRate)

    refilledAt = refilledAt + numRefills * interval
  end

  -- Only reject if tokens are 0 and we're consuming (not refunding)
  if tokens == 0 and incrementBy > 0 then
    return {-1, refilledAt + interval, effectiveLimit}
  end

  local remaining = tokens - incrementBy
  local expireAt = math.ceil(((effectiveLimit - remaining) / refillRate)) * interval
        
  redis.call("HSET", key, "refilledAt", refilledAt, "tokens", remaining)

  if (expireAt > 0) then
    redis.call("PEXPIRE", key, expireAt)
  end
  return {remaining, refilledAt + interval, effectiveLimit}
`;
    var tokenBucketIdentifierNotFound = -1;
    var tokenBucketRemainingTokensScript = `
  local key         = KEYS[1]
  local dynamicLimitKey = KEYS[2]       -- optional: key for dynamic limit in redis
  local maxTokens   = tonumber(ARGV[1]) -- default maximum number of tokens

  -- Check for dynamic limit
  local effectiveLimit = maxTokens
  if dynamicLimitKey ~= "" then
    local dynamicLimit = redis.call("GET", dynamicLimitKey)
    if dynamicLimit then
      effectiveLimit = tonumber(dynamicLimit)
    end
  end
        
  local bucket = redis.call("HMGET", key, "refilledAt", "tokens")

  if bucket[1] == false then
    return {effectiveLimit, ${tokenBucketIdentifierNotFound}, effectiveLimit}
  end
        
  return {tonumber(bucket[2]), tonumber(bucket[1]), effectiveLimit}
`;
    var cachedFixedWindowLimitScript = `
  local key     = KEYS[1]
  local window  = ARGV[1]
  local incrementBy   = ARGV[2] -- increment rate per request at a given value, default is 1

  local r = redis.call("INCRBY", key, incrementBy)
  if r == incrementBy then
  -- The first time this key is set, the value will be equal to incrementBy.
  -- So we only need the expire command once
  redis.call("PEXPIRE", key, window)
  end
      
  return r
`;
    var cachedFixedWindowRemainingTokenScript = `
  local key = KEYS[1]
  local tokens = 0

  local value = redis.call('GET', key)
  if value then
      tokens = value
  end
  return tokens
`;
    var fixedWindowLimitScript2 = `
	local key           = KEYS[1]
	local id            = ARGV[1]
	local window        = ARGV[2]
	local incrementBy   = tonumber(ARGV[3])

	redis.call("HSET", key, id, incrementBy)
	local fields = redis.call("HGETALL", key)
	if #fields == 2 and tonumber(fields[2])==incrementBy then
	-- The first time this key is set, and the value will be equal to incrementBy.
	-- So we only need the expire command once
	  redis.call("PEXPIRE", key, window)
	end

	return fields
`;
    var fixedWindowRemainingTokensScript2 = `
      local key = KEYS[1]
      local tokens = 0

      local fields = redis.call("HGETALL", key)

      return fields
    `;
    var slidingWindowLimitScript2 = `
	local currentKey    = KEYS[1]           -- identifier including prefixes
	local previousKey   = KEYS[2]           -- key of the previous bucket
	local tokens        = tonumber(ARGV[1]) -- tokens per window
	local now           = ARGV[2]           -- current timestamp in milliseconds
	local window        = ARGV[3]           -- interval in milliseconds
	local requestId     = ARGV[4]           -- uuid for this request
	local incrementBy   = tonumber(ARGV[5]) -- custom rate, default is  1

	local currentFields = redis.call("HGETALL", currentKey)
	local requestsInCurrentWindow = 0
	for i = 2, #currentFields, 2 do
	requestsInCurrentWindow = requestsInCurrentWindow + tonumber(currentFields[i])
	end

	local previousFields = redis.call("HGETALL", previousKey)
	local requestsInPreviousWindow = 0
	for i = 2, #previousFields, 2 do
	requestsInPreviousWindow = requestsInPreviousWindow + tonumber(previousFields[i])
	end

	local percentageInCurrent = ( now % window) / window

	-- Only check limit if not refunding (negative rate)
	if incrementBy > 0 and requestsInPreviousWindow * (1 - percentageInCurrent ) + requestsInCurrentWindow + incrementBy > tokens then
	  return {currentFields, previousFields, false}
	end

	redis.call("HSET", currentKey, requestId, incrementBy)

	if requestsInCurrentWindow == 0 then 
	  -- The first time this key is set, the value will be equal to incrementBy.
	  -- So we only need the expire command once
	  redis.call("PEXPIRE", currentKey, window * 2 + 1000) -- Enough time to overlap with a new window + 1 second
	end
	return {currentFields, previousFields, true}
`;
    var slidingWindowRemainingTokensScript2 = `
	local currentKey    = KEYS[1]           -- identifier including prefixes
	local previousKey   = KEYS[2]           -- key of the previous bucket
	local now         	= ARGV[1]           -- current timestamp in milliseconds
  	local window      	= ARGV[2]           -- interval in milliseconds

	local currentFields = redis.call("HGETALL", currentKey)
	local requestsInCurrentWindow = 0
	for i = 2, #currentFields, 2 do
	requestsInCurrentWindow = requestsInCurrentWindow + tonumber(currentFields[i])
	end

	local previousFields = redis.call("HGETALL", previousKey)
	local requestsInPreviousWindow = 0
	for i = 2, #previousFields, 2 do
	requestsInPreviousWindow = requestsInPreviousWindow + tonumber(previousFields[i])
	end

	local percentageInCurrent = ( now % window) / window
  	requestsInPreviousWindow = math.floor(( 1 - percentageInCurrent ) * requestsInPreviousWindow)
	
	return requestsInCurrentWindow + requestsInPreviousWindow
`;
    var resetScript = `
      local pattern = KEYS[1]

      -- Initialize cursor to start from 0
      local cursor = "0"

      repeat
          -- Scan for keys matching the pattern
          local scan_result = redis.call('SCAN', cursor, 'MATCH', pattern)

          -- Extract cursor for the next iteration
          cursor = scan_result[1]

          -- Extract keys from the scan result
          local keys = scan_result[2]

          for i=1, #keys do
          redis.call('DEL', keys[i])
          end

      -- Continue scanning until cursor is 0 (end of keyspace)
      until cursor == "0"
    `;
    var SCRIPTS = {
      singleRegion: {
        fixedWindow: {
          limit: {
            script: fixedWindowLimitScript,
            hash: "472e55443b62f60d0991028456c57815a387066d"
          },
          getRemaining: {
            script: fixedWindowRemainingTokensScript,
            hash: "40515c9dd0a08f8584f5f9b593935f6a87c1c1c3"
          }
        },
        slidingWindow: {
          limit: {
            script: slidingWindowLimitScript,
            hash: "977fb636fb5ceb7e98a96d1b3a1272ba018efdae"
          },
          getRemaining: {
            script: slidingWindowRemainingTokensScript,
            hash: "ee3a3265fad822f83acad23f8a1e2f5c0b156b03"
          }
        },
        tokenBucket: {
          limit: {
            script: tokenBucketLimitScript,
            hash: "b35c5bc0b7fdae7dd0573d4529911cabaf9d1d89"
          },
          getRemaining: {
            script: tokenBucketRemainingTokensScript,
            hash: "deb03663e8af5a968deee895dd081be553d2611b"
          }
        },
        cachedFixedWindow: {
          limit: {
            script: cachedFixedWindowLimitScript,
            hash: "c26b12703dd137939b9a69a3a9b18e906a2d940f"
          },
          getRemaining: {
            script: cachedFixedWindowRemainingTokenScript,
            hash: "8e8f222ccae68b595ee6e3f3bf2199629a62b91a"
          }
        }
      },
      multiRegion: {
        fixedWindow: {
          limit: {
            script: fixedWindowLimitScript2,
            hash: "a8c14f3835aa87bd70e5e2116081b81664abcf5c"
          },
          getRemaining: {
            script: fixedWindowRemainingTokensScript2,
            hash: "8ab8322d0ed5fe5ac8eb08f0c2e4557f1b4816fd"
          }
        },
        slidingWindow: {
          limit: {
            script: slidingWindowLimitScript2,
            hash: "1e7ca8dcd2d600a6d0124a67a57ea225ed62921b"
          },
          getRemaining: {
            script: slidingWindowRemainingTokensScript2,
            hash: "558c9306b7ec54abb50747fe0b17e5d44bd24868"
          }
        }
      }
    };
    var RESET_SCRIPT = {
      script: resetScript,
      hash: "54bd274ddc59fb3be0f42deee2f64322a10e2b50"
    };
    var DenyListExtension = "denyList";
    var IpDenyListKey = "ipDenyList";
    var IpDenyListStatusKey = "ipDenyListStatus";
    var checkDenyListScript = `
  -- Checks if values provideed in ARGV are present in the deny lists.
  -- This is done using the allDenyListsKey below.

  -- Additionally, checks the status of the ip deny list using the
  -- ipDenyListStatusKey below. Here are the possible states of the
  -- ipDenyListStatusKey key:
  -- * status == -1: set to "disabled" with no TTL
  -- * status == -2: not set, meaning that is was set before but expired
  -- * status  >  0: set to "valid", with a TTL
  --
  -- In the case of status == -2, we set the status to "pending" with
  -- 30 second ttl. During this time, the process which got status == -2
  -- will update the ip deny list.

  local allDenyListsKey     = KEYS[1]
  local ipDenyListStatusKey = KEYS[2]

  local results = redis.call('SMISMEMBER', allDenyListsKey, unpack(ARGV))
  local status  = redis.call('TTL', ipDenyListStatusKey)
  if status == -2 then
    redis.call('SETEX', ipDenyListStatusKey, 30, "pending")
  end

  return { results, status }
`;
    var ip_deny_list_exports = {};
    __export2(ip_deny_list_exports, {
      ThresholdError: () => ThresholdError,
      disableIpDenyList: () => disableIpDenyList,
      updateIpDenyList: () => updateIpDenyList
    });
    var MILLISECONDS_IN_HOUR = 60 * 60 * 1e3;
    var MILLISECONDS_IN_DAY = 24 * MILLISECONDS_IN_HOUR;
    var MILLISECONDS_TO_2AM = 2 * MILLISECONDS_IN_HOUR;
    var getIpListTTL = (time) => {
      const now = time || Date.now();
      const timeSinceLast2AM = (now - MILLISECONDS_TO_2AM) % MILLISECONDS_IN_DAY;
      return MILLISECONDS_IN_DAY - timeSinceLast2AM;
    };
    var baseUrl = "https://raw.githubusercontent.com/stamparm/ipsum/master/levels";
    var ThresholdError = class extends Error {
      constructor(threshold) {
        super(`Allowed threshold values are from 1 to 8, 1 and 8 included. Received: ${threshold}`);
        this.name = "ThresholdError";
      }
    };
    var getIpDenyList = async (threshold) => {
      if (typeof threshold !== "number" || threshold < 1 || threshold > 8) {
        throw new ThresholdError(threshold);
      }
      try {
        const response = await fetch(`${baseUrl}/${threshold}.txt`);
        if (!response.ok) {
          throw new Error(`Error fetching data: ${response.statusText}`);
        }
        const data = await response.text();
        const lines = data.split("\n");
        return lines.filter((value) => value.length > 0);
      } catch (error) {
        throw new Error(`Failed to fetch ip deny list: ${error}`);
      }
    };
    var updateIpDenyList = async (redis, prefix, threshold, ttl) => {
      const allIps = await getIpDenyList(threshold);
      const allDenyLists = [prefix, DenyListExtension, "all"].join(":");
      const ipDenyList = [prefix, DenyListExtension, IpDenyListKey].join(":");
      const statusKey = [prefix, IpDenyListStatusKey].join(":");
      const transaction = redis.multi();
      transaction.sdiffstore(allDenyLists, allDenyLists, ipDenyList);
      transaction.del(ipDenyList);
      transaction.sadd(ipDenyList, allIps.at(0), ...allIps.slice(1));
      transaction.sdiffstore(ipDenyList, ipDenyList, allDenyLists);
      transaction.sunionstore(allDenyLists, allDenyLists, ipDenyList);
      transaction.set(statusKey, "valid", { px: ttl ?? getIpListTTL() });
      return await transaction.exec();
    };
    var disableIpDenyList = async (redis, prefix) => {
      const allDenyListsKey = [prefix, DenyListExtension, "all"].join(":");
      const ipDenyListKey = [prefix, DenyListExtension, IpDenyListKey].join(":");
      const statusKey = [prefix, IpDenyListStatusKey].join(":");
      const transaction = redis.multi();
      transaction.sdiffstore(allDenyListsKey, allDenyListsKey, ipDenyListKey);
      transaction.del(ipDenyListKey);
      transaction.set(statusKey, "disabled");
      return await transaction.exec();
    };
    var denyListCache = new Cache(/* @__PURE__ */ new Map());
    var checkDenyListCache = (members) => {
      return members.find(
        (member) => denyListCache.isBlocked(member).blocked
      );
    };
    var blockMember = (member) => {
      if (denyListCache.size() > 1e3)
        denyListCache.empty();
      denyListCache.blockUntil(member, Date.now() + 6e4);
    };
    var checkDenyList = async (redis, prefix, members) => {
      const [deniedValues, ipDenyListStatus] = await redis.eval(
        checkDenyListScript,
        [
          [prefix, DenyListExtension, "all"].join(":"),
          [prefix, IpDenyListStatusKey].join(":")
        ],
        members
      );
      let deniedValue = void 0;
      deniedValues.map((memberDenied, index) => {
        if (memberDenied) {
          blockMember(members[index]);
          deniedValue = members[index];
        }
      });
      return {
        deniedValue,
        invalidIpDenyList: ipDenyListStatus === -2
      };
    };
    var resolveLimitPayload = (redis, prefix, [ratelimitResponse, denyListResponse], threshold) => {
      if (denyListResponse.deniedValue) {
        ratelimitResponse.success = false;
        ratelimitResponse.remaining = 0;
        ratelimitResponse.reason = "denyList";
        ratelimitResponse.deniedValue = denyListResponse.deniedValue;
      }
      if (denyListResponse.invalidIpDenyList) {
        const updatePromise = updateIpDenyList(redis, prefix, threshold);
        ratelimitResponse.pending = Promise.all([
          ratelimitResponse.pending,
          updatePromise
        ]);
      }
      return ratelimitResponse;
    };
    var defaultDeniedResponse = (deniedValue) => {
      return {
        success: false,
        limit: 0,
        remaining: 0,
        reset: 0,
        pending: Promise.resolve(),
        reason: "denyList",
        deniedValue
      };
    };
    var Ratelimit2 = class {
      limiter;
      ctx;
      prefix;
      timeout;
      primaryRedis;
      analytics;
      enableProtection;
      denyListThreshold;
      dynamicLimits;
      constructor(config2) {
        this.ctx = config2.ctx;
        this.limiter = config2.limiter;
        this.timeout = config2.timeout ?? 5e3;
        this.prefix = config2.prefix ?? DEFAULT_PREFIX;
        this.dynamicLimits = config2.dynamicLimits ?? false;
        this.enableProtection = config2.enableProtection ?? false;
        this.denyListThreshold = config2.denyListThreshold ?? 6;
        this.primaryRedis = "redis" in this.ctx ? this.ctx.redis : this.ctx.regionContexts[0].redis;
        if ("redis" in this.ctx) {
          this.ctx.dynamicLimits = this.dynamicLimits;
          this.ctx.prefix = this.prefix;
        }
        this.analytics = config2.analytics ? new Analytics2({
          redis: this.primaryRedis,
          prefix: this.prefix
        }) : void 0;
        if (config2.ephemeralCache instanceof Map) {
          this.ctx.cache = new Cache(config2.ephemeralCache);
        } else if (config2.ephemeralCache === void 0) {
          this.ctx.cache = new Cache(/* @__PURE__ */ new Map());
        }
      }
      /**
       * Determine if a request should pass or be rejected based on the identifier and previously chosen ratelimit.
       *
       * Use this if you want to reject all requests that you can not handle right now.
       *
       * @example
       * ```ts
       *  const ratelimit = new Ratelimit({
       *    redis: Redis.fromEnv(),
       *    limiter: Ratelimit.slidingWindow(10, "10 s")
       *  })
       *
       *  const { success } = await ratelimit.limit(id)
       *  if (!success){
       *    return "Nope"
       *  }
       *  return "Yes"
       * ```
       *
       * @param req.rate - The rate at which tokens will be added or consumed from the token bucket. A higher rate allows for more requests to be processed. Defaults to 1 token per interval if not specified.
       *
       * Usage with `req.rate`
       * @example
       * ```ts
       *  const ratelimit = new Ratelimit({
       *    redis: Redis.fromEnv(),
       *    limiter: Ratelimit.slidingWindow(100, "10 s")
       *  })
       *
       *  const { success } = await ratelimit.limit(id, {rate: 10})
       *  if (!success){
       *    return "Nope"
       *  }
       *  return "Yes"
       * ```
       */
      limit = async (identifier, req) => {
        let timeoutId = null;
        try {
          const response = this.getRatelimitResponse(identifier, req);
          const { responseArray, newTimeoutId } = this.applyTimeout(response);
          timeoutId = newTimeoutId;
          const timedResponse = await Promise.race(responseArray);
          const finalResponse = this.submitAnalytics(timedResponse, identifier, req);
          return finalResponse;
        } finally {
          if (timeoutId) {
            clearTimeout(timeoutId);
          }
        }
      };
      /**
       * Block until the request may pass or timeout is reached.
       *
       * This method returns a promise that resolves as soon as the request may be processed
       * or after the timeout has been reached.
       *
       * Use this if you want to delay the request until it is ready to get processed.
       *
       * @example
       * ```ts
       *  const ratelimit = new Ratelimit({
       *    redis: Redis.fromEnv(),
       *    limiter: Ratelimit.slidingWindow(10, "10 s")
       *  })
       *
       *  const { success } = await ratelimit.blockUntilReady(id, 60_000)
       *  if (!success){
       *    return "Nope"
       *  }
       *  return "Yes"
       * ```
       */
      blockUntilReady = async (identifier, timeout) => {
        if (timeout <= 0) {
          throw new Error("timeout must be positive");
        }
        let res;
        const deadline = Date.now() + timeout;
        while (true) {
          res = await this.limit(identifier);
          if (res.success) {
            break;
          }
          if (res.reset === 0) {
            throw new Error("This should not happen");
          }
          const wait = Math.min(res.reset, deadline) - Date.now();
          await new Promise((r) => setTimeout(r, wait));
          if (Date.now() > deadline) {
            break;
          }
        }
        return res;
      };
      resetUsedTokens = async (identifier) => {
        const pattern = [this.prefix, identifier].join(":");
        await this.limiter().resetTokens(this.ctx, pattern);
      };
      /**
       * Returns the remaining token count together with a reset timestamps
       * 
       * @param identifier identifir to check
       * @returns object with `remaining`, `reset`, and `limit` fields. `remaining` denotes
       *          the remaining tokens, `limit` is the effective limit (considering dynamic
       *          limits if enabled), and `reset` denotes the timestamp when the tokens reset.
       */
      getRemaining = async (identifier) => {
        const pattern = [this.prefix, identifier].join(":");
        return await this.limiter().getRemaining(this.ctx, pattern);
      };
      /**
       * Checks if the identifier or the values in req are in the deny list cache.
       * If so, returns the default denied response.
       * 
       * Otherwise, calls redis to check the rate limit and deny list. Returns after
       * resolving the result. Resolving is overriding the rate limit result if
       * the some value is in deny list.
       * 
       * @param identifier identifier to block
       * @param req options with ip, user agent, country, rate and geo info
       * @returns rate limit response
       */
      getRatelimitResponse = async (identifier, req) => {
        const key = this.getKey(identifier);
        const definedMembers = this.getDefinedMembers(identifier, req);
        const deniedValue = checkDenyListCache(definedMembers);
        const result = deniedValue ? [defaultDeniedResponse(deniedValue), { deniedValue, invalidIpDenyList: false }] : await Promise.all([
          this.limiter().limit(this.ctx, key, req?.rate),
          this.enableProtection ? checkDenyList(this.primaryRedis, this.prefix, definedMembers) : { deniedValue: void 0, invalidIpDenyList: false }
        ]);
        return resolveLimitPayload(this.primaryRedis, this.prefix, result, this.denyListThreshold);
      };
      /**
       * Creates an array with the original response promise and a timeout promise
       * if this.timeout > 0.
       * 
       * @param response Ratelimit response promise
       * @returns array with the response and timeout promise. also includes the timeout id
       */
      applyTimeout = (response) => {
        let newTimeoutId = null;
        const responseArray = [response];
        if (this.timeout > 0) {
          const timeoutResponse = new Promise((resolve) => {
            newTimeoutId = setTimeout(() => {
              resolve({
                success: true,
                limit: 0,
                remaining: 0,
                reset: 0,
                pending: Promise.resolve(),
                reason: "timeout"
              });
            }, this.timeout);
          });
          responseArray.push(timeoutResponse);
        }
        return {
          responseArray,
          newTimeoutId
        };
      };
      /**
       * submits analytics if this.analytics is set
       * 
       * @param ratelimitResponse final rate limit response
       * @param identifier identifier to submit
       * @param req limit options
       * @returns rate limit response after updating the .pending field
       */
      submitAnalytics = (ratelimitResponse, identifier, req) => {
        if (this.analytics) {
          try {
            const geo = req ? this.analytics.extractGeo(req) : void 0;
            const analyticsP = this.analytics.record({
              identifier: ratelimitResponse.reason === "denyList" ? ratelimitResponse.deniedValue : identifier,
              time: Date.now(),
              success: ratelimitResponse.reason === "denyList" ? "denied" : ratelimitResponse.success,
              ...geo
            }).catch((error) => {
              let errorMessage = "Failed to record analytics";
              if (`${error}`.includes("WRONGTYPE")) {
                errorMessage = `
    Failed to record analytics. See the information below:

    This can occur when you uprade to Ratelimit version 1.1.2
    or later from an earlier version.

    This occurs simply because the way we store analytics data
    has changed. To avoid getting this error, disable analytics
    for *an hour*, then simply enable it back.

    `;
              }
              console.warn(errorMessage, error);
            });
            ratelimitResponse.pending = Promise.all([ratelimitResponse.pending, analyticsP]);
          } catch (error) {
            console.warn("Failed to record analytics", error);
          }
          ;
        }
        ;
        return ratelimitResponse;
      };
      getKey = (identifier) => {
        return [this.prefix, identifier].join(":");
      };
      /**
       * returns a list of defined values from
       * [identifier, req.ip, req.userAgent, req.country]
       * 
       * @param identifier identifier
       * @param req limit options
       * @returns list of defined values
       */
      getDefinedMembers = (identifier, req) => {
        const members = [identifier, req?.ip, req?.userAgent, req?.country];
        return members.filter(Boolean);
      };
      /**
       * Set a dynamic rate limit globally.
       * 
       * When dynamicLimits is enabled, this limit will override the default limit
       * set in the constructor for all requests.
       * 
       * @example
       * ```ts
       * const ratelimit = new Ratelimit({
       *   redis: Redis.fromEnv(),
       *   limiter: Ratelimit.slidingWindow(10, "10 s"),
       *   dynamicLimits: true
       * });
       * 
       * // Set global dynamic limit to 120 requests
       * await ratelimit.setDynamicLimit({ limit: 120 });
       * 
       * // Disable dynamic limit (falls back to default)
       * await ratelimit.setDynamicLimit({ limit: false });
       * ```
       * 
       * @param options.limit - The new rate limit to apply globally, or false to disable
       */
      setDynamicLimit = async (options) => {
        if (!this.dynamicLimits) {
          throw new Error(
            "dynamicLimits must be enabled in the Ratelimit constructor to use setDynamicLimit()"
          );
        }
        const globalKey = `${this.prefix}${DYNAMIC_LIMIT_KEY_SUFFIX}`;
        await (options.limit === false ? this.primaryRedis.del(globalKey) : this.primaryRedis.set(globalKey, options.limit));
      };
      /**
       * Get the current global dynamic rate limit.
       * 
       * @example
       * ```ts
       * const { dynamicLimit } = await ratelimit.getDynamicLimit();
       * console.log(dynamicLimit); // 120 or null if not set
       * ```
       * 
       * @returns Object containing the current global dynamic limit, or null if not set
       */
      getDynamicLimit = async () => {
        if (!this.dynamicLimits) {
          throw new Error(
            "dynamicLimits must be enabled in the Ratelimit constructor to use getDynamicLimit()"
          );
        }
        const globalKey = `${this.prefix}${DYNAMIC_LIMIT_KEY_SUFFIX}`;
        const result = await this.primaryRedis.get(globalKey);
        return { dynamicLimit: result === null ? null : Number(result) };
      };
    };
    function randomId() {
      let result = "";
      const characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
      const charactersLength = characters.length;
      for (let i = 0; i < 16; i++) {
        result += characters.charAt(Math.floor(Math.random() * charactersLength));
      }
      return result;
    }
    var MultiRegionRatelimit = class extends Ratelimit2 {
      /**
       * Create a new Ratelimit instance by providing a `@upstash/redis` instance and the algorithn of your choice.
       */
      constructor(config2) {
        super({
          prefix: config2.prefix,
          limiter: config2.limiter,
          timeout: config2.timeout,
          analytics: config2.analytics,
          dynamicLimits: config2.dynamicLimits,
          ctx: {
            regionContexts: config2.redis.map((redis) => ({
              redis,
              prefix: config2.prefix ?? DEFAULT_PREFIX
            })),
            cache: config2.ephemeralCache ? new Cache(config2.ephemeralCache) : void 0
          }
        });
        if (config2.dynamicLimits) {
          console.warn(
            "Warning: Dynamic limits are not yet supported for multi-region rate limiters. The dynamicLimits option will be ignored."
          );
        }
      }
      /**
       * Each request inside a fixed time increases a counter.
       * Once the counter reaches the maximum allowed number, all further requests are
       * rejected.
       *
       * **Pro:**
       *
       * - Newer requests are not starved by old ones.
       * - Low storage cost.
       *
       * **Con:**
       *
       * A burst of requests near the boundary of a window can result in a very
       * high request rate because two windows will be filled with requests quickly.
       *
       * @param tokens - How many requests a user can make in each time window.
       * @param window - A fixed timeframe
       */
      static fixedWindow(tokens, window2) {
        const windowDuration = ms(window2);
        return () => ({
          async limit(ctx, identifier, rate) {
            const requestId = randomId();
            const bucket = Math.floor(Date.now() / windowDuration);
            const key = [identifier, bucket].join(":");
            const incrementBy = rate ?? 1;
            if (ctx.cache && incrementBy > 0) {
              const { blocked, reset: reset2 } = ctx.cache.isBlocked(identifier);
              if (blocked) {
                return {
                  success: false,
                  limit: tokens,
                  remaining: 0,
                  reset: reset2,
                  pending: Promise.resolve(),
                  reason: "cacheBlock"
                };
              }
            }
            const dbs = ctx.regionContexts.map((regionContext) => ({
              redis: regionContext.redis,
              request: safeEval(
                regionContext,
                SCRIPTS.multiRegion.fixedWindow.limit,
                [key],
                [requestId, windowDuration, incrementBy]
              )
            }));
            const firstResponse = await Promise.any(dbs.map((s) => s.request));
            const usedTokens = firstResponse.reduce(
              (accTokens, usedToken, index) => {
                let parsedToken = 0;
                if (index % 2) {
                  parsedToken = Number.parseInt(usedToken);
                }
                return accTokens + parsedToken;
              },
              0
            );
            const remaining = tokens - usedTokens;
            async function sync() {
              const individualIDs = await Promise.all(dbs.map((s) => s.request));
              const allIDs = [
                ...new Set(
                  individualIDs.flat().reduce((acc, curr, index) => {
                    if (index % 2 === 0) {
                      acc.push(curr);
                    }
                    return acc;
                  }, [])
                ).values()
              ];
              for (const db of dbs) {
                const usedDbTokensRequest = await db.request;
                const usedDbTokens = usedDbTokensRequest.reduce(
                  (accTokens, usedToken, index) => {
                    let parsedToken = 0;
                    if (index % 2) {
                      parsedToken = Number.parseInt(usedToken);
                    }
                    return accTokens + parsedToken;
                  },
                  0
                );
                const dbIdsRequest = await db.request;
                const dbIds = dbIdsRequest.reduce(
                  (ids, currentId, index) => {
                    if (index % 2 === 0) {
                      ids.push(currentId);
                    }
                    return ids;
                  },
                  []
                );
                if (usedDbTokens >= tokens) {
                  continue;
                }
                const diff = allIDs.filter((id) => !dbIds.includes(id));
                if (diff.length === 0) {
                  continue;
                }
                for (const requestId2 of diff) {
                  await db.redis.hset(key, { [requestId2]: incrementBy });
                }
              }
            }
            const success = remaining >= 0;
            const reset = (bucket + 1) * windowDuration;
            if (ctx.cache) {
              if (!success) {
                ctx.cache.blockUntil(identifier, reset);
              } else if (incrementBy < 0) {
                ctx.cache.pop(identifier);
              }
            }
            return {
              success,
              limit: tokens,
              remaining,
              reset,
              pending: sync()
            };
          },
          async getRemaining(ctx, identifier) {
            const bucket = Math.floor(Date.now() / windowDuration);
            const key = [identifier, bucket].join(":");
            const dbs = ctx.regionContexts.map((regionContext) => ({
              redis: regionContext.redis,
              request: safeEval(
                regionContext,
                SCRIPTS.multiRegion.fixedWindow.getRemaining,
                [key],
                [null]
              )
            }));
            const firstResponse = await Promise.any(dbs.map((s) => s.request));
            const usedTokens = firstResponse.reduce(
              (accTokens, usedToken, index) => {
                let parsedToken = 0;
                if (index % 2) {
                  parsedToken = Number.parseInt(usedToken);
                }
                return accTokens + parsedToken;
              },
              0
            );
            return {
              remaining: Math.max(0, tokens - usedTokens),
              reset: (bucket + 1) * windowDuration,
              limit: tokens
            };
          },
          async resetTokens(ctx, identifier) {
            const pattern = [identifier, "*"].join(":");
            if (ctx.cache) {
              ctx.cache.pop(identifier);
            }
            await Promise.all(
              ctx.regionContexts.map((regionContext) => {
                safeEval(regionContext, RESET_SCRIPT, [pattern], [null]);
              })
            );
          }
        });
      }
      /**
       * Combined approach of `slidingLogs` and `fixedWindow` with lower storage
       * costs than `slidingLogs` and improved boundary behavior by calculating a
       * weighted score between two windows.
       *
       * **Pro:**
       *
       * Good performance allows this to scale to very high loads.
       *
       * **Con:**
       *
       * Nothing major.
       *
       * @param tokens - How many requests a user can make in each time window.
       * @param window - The duration in which the user can max X requests.
       */
      static slidingWindow(tokens, window2) {
        const windowSize = ms(window2);
        const windowDuration = ms(window2);
        return () => ({
          async limit(ctx, identifier, rate) {
            const requestId = randomId();
            const now = Date.now();
            const currentWindow = Math.floor(now / windowSize);
            const currentKey = [identifier, currentWindow].join(":");
            const previousWindow = currentWindow - 1;
            const previousKey = [identifier, previousWindow].join(":");
            const incrementBy = rate ?? 1;
            if (ctx.cache && incrementBy > 0) {
              const { blocked, reset: reset2 } = ctx.cache.isBlocked(identifier);
              if (blocked) {
                return {
                  success: false,
                  limit: tokens,
                  remaining: 0,
                  reset: reset2,
                  pending: Promise.resolve(),
                  reason: "cacheBlock"
                };
              }
            }
            const dbs = ctx.regionContexts.map((regionContext) => ({
              redis: regionContext.redis,
              request: safeEval(
                regionContext,
                SCRIPTS.multiRegion.slidingWindow.limit,
                [currentKey, previousKey],
                [tokens, now, windowDuration, requestId, incrementBy]
                // lua seems to return `1` for true and `null` for false
              )
            }));
            const percentageInCurrent = now % windowDuration / windowDuration;
            const [current, previous, success] = await Promise.any(
              dbs.map((s) => s.request)
            );
            if (success) {
              current.push(requestId, incrementBy.toString());
            }
            const previousUsedTokens = previous.reduce(
              (accTokens, usedToken, index) => {
                let parsedToken = 0;
                if (index % 2) {
                  parsedToken = Number.parseInt(usedToken);
                }
                return accTokens + parsedToken;
              },
              0
            );
            const currentUsedTokens = current.reduce(
              (accTokens, usedToken, index) => {
                let parsedToken = 0;
                if (index % 2) {
                  parsedToken = Number.parseInt(usedToken);
                }
                return accTokens + parsedToken;
              },
              0
            );
            const previousPartialUsed = Math.ceil(
              previousUsedTokens * (1 - percentageInCurrent)
            );
            const usedTokens = previousPartialUsed + currentUsedTokens;
            const remaining = tokens - usedTokens;
            async function sync() {
              const res = await Promise.all(dbs.map((s) => s.request));
              const allCurrentIds = [
                ...new Set(
                  res.flatMap(([current2]) => current2).reduce((acc, curr, index) => {
                    if (index % 2 === 0) {
                      acc.push(curr);
                    }
                    return acc;
                  }, [])
                ).values()
              ];
              for (const db of dbs) {
                const [current2, _previous, _success] = await db.request;
                const dbIds = current2.reduce((ids, currentId, index) => {
                  if (index % 2 === 0) {
                    ids.push(currentId);
                  }
                  return ids;
                }, []);
                const usedDbTokens = current2.reduce(
                  (accTokens, usedToken, index) => {
                    let parsedToken = 0;
                    if (index % 2) {
                      parsedToken = Number.parseInt(usedToken);
                    }
                    return accTokens + parsedToken;
                  },
                  0
                );
                if (usedDbTokens >= tokens) {
                  continue;
                }
                const diff = allCurrentIds.filter((id) => !dbIds.includes(id));
                if (diff.length === 0) {
                  continue;
                }
                for (const requestId2 of diff) {
                  await db.redis.hset(currentKey, { [requestId2]: incrementBy });
                }
              }
            }
            const reset = (currentWindow + 1) * windowDuration;
            if (ctx.cache) {
              if (!success) {
                ctx.cache.blockUntil(identifier, reset);
              } else if (incrementBy < 0) {
                ctx.cache.pop(identifier);
              }
            }
            return {
              success: Boolean(success),
              limit: tokens,
              remaining: Math.max(0, remaining),
              reset,
              pending: sync()
            };
          },
          async getRemaining(ctx, identifier) {
            const now = Date.now();
            const currentWindow = Math.floor(now / windowSize);
            const currentKey = [identifier, currentWindow].join(":");
            const previousWindow = currentWindow - 1;
            const previousKey = [identifier, previousWindow].join(":");
            const dbs = ctx.regionContexts.map((regionContext) => ({
              redis: regionContext.redis,
              request: safeEval(
                regionContext,
                SCRIPTS.multiRegion.slidingWindow.getRemaining,
                [currentKey, previousKey],
                [now, windowSize]
                // lua seems to return `1` for true and `null` for false
              )
            }));
            const usedTokens = await Promise.any(dbs.map((s) => s.request));
            return {
              remaining: Math.max(0, tokens - usedTokens),
              reset: (currentWindow + 1) * windowSize,
              limit: tokens
            };
          },
          async resetTokens(ctx, identifier) {
            const pattern = [identifier, "*"].join(":");
            if (ctx.cache) {
              ctx.cache.pop(identifier);
            }
            await Promise.all(
              ctx.regionContexts.map((regionContext) => {
                safeEval(regionContext, RESET_SCRIPT, [pattern], [null]);
              })
            );
          }
        });
      }
    };
    var RegionRatelimit = class extends Ratelimit2 {
      /**
       * Create a new Ratelimit instance by providing a `@upstash/redis` instance and the algorithm of your choice.
       */
      constructor(config2) {
        super({
          prefix: config2.prefix,
          limiter: config2.limiter,
          timeout: config2.timeout,
          analytics: config2.analytics,
          ctx: {
            redis: config2.redis,
            prefix: config2.prefix ?? DEFAULT_PREFIX
          },
          ephemeralCache: config2.ephemeralCache,
          enableProtection: config2.enableProtection,
          denyListThreshold: config2.denyListThreshold,
          dynamicLimits: config2.dynamicLimits
        });
      }
      /**
       * Each request inside a fixed time increases a counter.
       * Once the counter reaches the maximum allowed number, all further requests are
       * rejected.
       *
       * **Pro:**
       *
       * - Newer requests are not starved by old ones.
       * - Low storage cost.
       *
       * **Con:**
       *
       * A burst of requests near the boundary of a window can result in a very
       * high request rate because two windows will be filled with requests quickly.
       *
       * @param tokens - How many requests a user can make in each time window.
       * @param window - A fixed timeframe
       */
      static fixedWindow(tokens, window2) {
        const windowDuration = ms(window2);
        return () => ({
          async limit(ctx, identifier, rate) {
            const bucket = Math.floor(Date.now() / windowDuration);
            const key = [identifier, bucket].join(":");
            const incrementBy = rate ?? 1;
            if (ctx.cache && incrementBy > 0) {
              const { blocked, reset: reset2 } = ctx.cache.isBlocked(identifier);
              if (blocked) {
                return {
                  success: false,
                  limit: tokens,
                  remaining: 0,
                  reset: reset2,
                  pending: Promise.resolve(),
                  reason: "cacheBlock"
                };
              }
            }
            const dynamicLimitKey = ctx.dynamicLimits ? `${ctx.prefix}${DYNAMIC_LIMIT_KEY_SUFFIX}` : "";
            const [usedTokensAfterUpdate, effectiveLimit] = await safeEval(
              ctx,
              SCRIPTS.singleRegion.fixedWindow.limit,
              [key, dynamicLimitKey],
              [tokens, windowDuration, incrementBy]
            );
            const success = usedTokensAfterUpdate <= effectiveLimit;
            const remainingTokens = Math.max(0, effectiveLimit - usedTokensAfterUpdate);
            const reset = (bucket + 1) * windowDuration;
            if (ctx.cache) {
              if (!success) {
                ctx.cache.blockUntil(identifier, reset);
              } else if (incrementBy < 0) {
                ctx.cache.pop(identifier);
              }
            }
            return {
              success,
              limit: effectiveLimit,
              remaining: remainingTokens,
              reset,
              pending: Promise.resolve()
            };
          },
          async getRemaining(ctx, identifier) {
            const bucket = Math.floor(Date.now() / windowDuration);
            const key = [identifier, bucket].join(":");
            const dynamicLimitKey = ctx.dynamicLimits ? `${ctx.prefix}${DYNAMIC_LIMIT_KEY_SUFFIX}` : "";
            const [remaining, effectiveLimit] = await safeEval(
              ctx,
              SCRIPTS.singleRegion.fixedWindow.getRemaining,
              [key, dynamicLimitKey],
              [tokens]
            );
            return {
              remaining: Math.max(0, remaining),
              reset: (bucket + 1) * windowDuration,
              limit: effectiveLimit
            };
          },
          async resetTokens(ctx, identifier) {
            const pattern = [identifier, "*"].join(":");
            if (ctx.cache) {
              ctx.cache.pop(identifier);
            }
            await safeEval(
              ctx,
              RESET_SCRIPT,
              [pattern],
              [null]
            );
          }
        });
      }
      /**
       * Combined approach of `slidingLogs` and `fixedWindow` with lower storage
       * costs than `slidingLogs` and improved boundary behavior by calculating a
       * weighted score between two windows.
       *
       * **Pro:**
       *
       * Good performance allows this to scale to very high loads.
       *
       * **Con:**
       *
       * Nothing major.
       *
       * @param tokens - How many requests a user can make in each time window.
       * @param window - The duration in which the user can max X requests.
       */
      static slidingWindow(tokens, window2) {
        const windowSize = ms(window2);
        return () => ({
          async limit(ctx, identifier, rate) {
            const now = Date.now();
            const currentWindow = Math.floor(now / windowSize);
            const currentKey = [identifier, currentWindow].join(":");
            const previousWindow = currentWindow - 1;
            const previousKey = [identifier, previousWindow].join(":");
            const incrementBy = rate ?? 1;
            if (ctx.cache && incrementBy > 0) {
              const { blocked, reset: reset2 } = ctx.cache.isBlocked(identifier);
              if (blocked) {
                return {
                  success: false,
                  limit: tokens,
                  remaining: 0,
                  reset: reset2,
                  pending: Promise.resolve(),
                  reason: "cacheBlock"
                };
              }
            }
            const dynamicLimitKey = ctx.dynamicLimits ? `${ctx.prefix}${DYNAMIC_LIMIT_KEY_SUFFIX}` : "";
            const [remainingTokens, effectiveLimit] = await safeEval(
              ctx,
              SCRIPTS.singleRegion.slidingWindow.limit,
              [currentKey, previousKey, dynamicLimitKey],
              [tokens, now, windowSize, incrementBy]
            );
            const success = remainingTokens >= 0;
            const reset = (currentWindow + 1) * windowSize;
            if (ctx.cache) {
              if (!success) {
                ctx.cache.blockUntil(identifier, reset);
              } else if (incrementBy < 0) {
                ctx.cache.pop(identifier);
              }
            }
            return {
              success,
              limit: effectiveLimit,
              remaining: Math.max(0, remainingTokens),
              reset,
              pending: Promise.resolve()
            };
          },
          async getRemaining(ctx, identifier) {
            const now = Date.now();
            const currentWindow = Math.floor(now / windowSize);
            const currentKey = [identifier, currentWindow].join(":");
            const previousWindow = currentWindow - 1;
            const previousKey = [identifier, previousWindow].join(":");
            const dynamicLimitKey = ctx.dynamicLimits ? `${ctx.prefix}${DYNAMIC_LIMIT_KEY_SUFFIX}` : "";
            const [remaining, effectiveLimit] = await safeEval(
              ctx,
              SCRIPTS.singleRegion.slidingWindow.getRemaining,
              [currentKey, previousKey, dynamicLimitKey],
              [tokens, now, windowSize]
            );
            return {
              remaining: Math.max(0, remaining),
              reset: (currentWindow + 1) * windowSize,
              limit: effectiveLimit
            };
          },
          async resetTokens(ctx, identifier) {
            const pattern = [identifier, "*"].join(":");
            if (ctx.cache) {
              ctx.cache.pop(identifier);
            }
            await safeEval(
              ctx,
              RESET_SCRIPT,
              [pattern],
              [null]
            );
          }
        });
      }
      /**
       * You have a bucket filled with `{maxTokens}` tokens that refills constantly
       * at `{refillRate}` per `{interval}`.
       * Every request will remove one token from the bucket and if there is no
       * token to take, the request is rejected.
       *
       * **Pro:**
       *
       * - Bursts of requests are smoothed out and you can process them at a constant
       * rate.
       * - Allows to set a higher initial burst limit by setting `maxTokens` higher
       * than `refillRate`
       */
      static tokenBucket(refillRate, interval, maxTokens) {
        const intervalDuration = ms(interval);
        return () => ({
          async limit(ctx, identifier, rate) {
            const now = Date.now();
            const incrementBy = rate ?? 1;
            if (ctx.cache && incrementBy > 0) {
              const { blocked, reset: reset2 } = ctx.cache.isBlocked(identifier);
              if (blocked) {
                return {
                  success: false,
                  limit: maxTokens,
                  remaining: 0,
                  reset: reset2,
                  pending: Promise.resolve(),
                  reason: "cacheBlock"
                };
              }
            }
            const dynamicLimitKey = ctx.dynamicLimits ? `${ctx.prefix}${DYNAMIC_LIMIT_KEY_SUFFIX}` : "";
            const [remaining, reset, effectiveLimit] = await safeEval(
              ctx,
              SCRIPTS.singleRegion.tokenBucket.limit,
              [identifier, dynamicLimitKey],
              [maxTokens, intervalDuration, refillRate, now, incrementBy]
            );
            const success = remaining >= 0;
            if (ctx.cache) {
              if (!success) {
                ctx.cache.blockUntil(identifier, reset);
              } else if (incrementBy < 0) {
                ctx.cache.pop(identifier);
              }
            }
            return {
              success,
              limit: effectiveLimit,
              remaining: Math.max(0, remaining),
              reset,
              pending: Promise.resolve()
            };
          },
          async getRemaining(ctx, identifier) {
            const dynamicLimitKey = ctx.dynamicLimits ? `${ctx.prefix}${DYNAMIC_LIMIT_KEY_SUFFIX}` : "";
            const [remainingTokens, refilledAt, effectiveLimit] = await safeEval(
              ctx,
              SCRIPTS.singleRegion.tokenBucket.getRemaining,
              [identifier, dynamicLimitKey],
              [maxTokens]
            );
            const freshRefillAt = Date.now() + intervalDuration;
            const identifierRefillsAt = refilledAt + intervalDuration;
            return {
              remaining: Math.max(0, remainingTokens),
              reset: refilledAt === tokenBucketIdentifierNotFound ? freshRefillAt : identifierRefillsAt,
              limit: effectiveLimit
            };
          },
          async resetTokens(ctx, identifier) {
            const pattern = identifier;
            if (ctx.cache) {
              ctx.cache.pop(identifier);
            }
            await safeEval(
              ctx,
              RESET_SCRIPT,
              [pattern],
              [null]
            );
          }
        });
      }
      /**
       * cachedFixedWindow first uses the local cache to decide if a request may pass and then updates
       * it asynchronously.
       * This is experimental and not yet recommended for production use.
       *
       * @experimental
       *
       * Each request inside a fixed time increases a counter.
       * Once the counter reaches the maximum allowed number, all further requests are
       * rejected.
       *
       * **Pro:**
       *
       * - Newer requests are not starved by old ones.
       * - Low storage cost.
       *
       * **Con:**
       *
       * A burst of requests near the boundary of a window can result in a very
       * high request rate because two windows will be filled with requests quickly.
       *
       * @param tokens - How many requests a user can make in each time window.
       * @param window - A fixed timeframe
       */
      static cachedFixedWindow(tokens, window2) {
        const windowDuration = ms(window2);
        return () => ({
          async limit(ctx, identifier, rate) {
            if (!ctx.cache) {
              throw new Error("This algorithm requires a cache");
            }
            if (ctx.dynamicLimits) {
              console.warn(
                "Warning: Dynamic limits are not yet supported for cachedFixedWindow algorithm. The dynamicLimits option will be ignored."
              );
            }
            const bucket = Math.floor(Date.now() / windowDuration);
            const key = [identifier, bucket].join(":");
            const reset = (bucket + 1) * windowDuration;
            const incrementBy = rate ?? 1;
            const hit = typeof ctx.cache.get(key) === "number";
            if (hit) {
              const cachedTokensAfterUpdate = ctx.cache.incr(key, incrementBy);
              const success = cachedTokensAfterUpdate < tokens;
              const pending = success ? safeEval(
                ctx,
                SCRIPTS.singleRegion.cachedFixedWindow.limit,
                [key],
                [windowDuration, incrementBy]
              ) : Promise.resolve();
              return {
                success,
                limit: tokens,
                remaining: tokens - cachedTokensAfterUpdate,
                reset,
                pending
              };
            }
            const usedTokensAfterUpdate = await safeEval(
              ctx,
              SCRIPTS.singleRegion.cachedFixedWindow.limit,
              [key],
              [windowDuration, incrementBy]
            );
            ctx.cache.set(key, usedTokensAfterUpdate);
            const remaining = tokens - usedTokensAfterUpdate;
            return {
              success: remaining >= 0,
              limit: tokens,
              remaining,
              reset,
              pending: Promise.resolve()
            };
          },
          async getRemaining(ctx, identifier) {
            if (!ctx.cache) {
              throw new Error("This algorithm requires a cache");
            }
            const bucket = Math.floor(Date.now() / windowDuration);
            const key = [identifier, bucket].join(":");
            const hit = typeof ctx.cache.get(key) === "number";
            if (hit) {
              const cachedUsedTokens = ctx.cache.get(key) ?? 0;
              return {
                remaining: Math.max(0, tokens - cachedUsedTokens),
                reset: (bucket + 1) * windowDuration,
                limit: tokens
              };
            }
            const usedTokens = await safeEval(
              ctx,
              SCRIPTS.singleRegion.cachedFixedWindow.getRemaining,
              [key],
              [null]
            );
            return {
              remaining: Math.max(0, tokens - usedTokens),
              reset: (bucket + 1) * windowDuration,
              limit: tokens
            };
          },
          async resetTokens(ctx, identifier) {
            if (!ctx.cache) {
              throw new Error("This algorithm requires a cache");
            }
            const bucket = Math.floor(Date.now() / windowDuration);
            const key = [identifier, bucket].join(":");
            ctx.cache.pop(key);
            const pattern = [identifier, "*"].join(":");
            await safeEval(
              ctx,
              RESET_SCRIPT,
              [pattern],
              [null]
            );
          }
        });
      }
    };
  }
});

// node_modules/papaparse/papaparse.js
var require_papaparse = __commonJS({
  "node_modules/papaparse/papaparse.js"(exports2, module) {
    (function(root, factory) {
      if (typeof define === "function" && define.amd) {
        define([], factory);
      } else if (typeof module === "object" && typeof exports2 !== "undefined") {
        module.exports = factory();
      } else {
        root.Papa = factory();
      }
    })(exports2, function moduleFactory() {
      "use strict";
      var global2 = (function() {
        if (typeof self !== "undefined") {
          return self;
        }
        if (typeof window !== "undefined") {
          return window;
        }
        if (typeof global2 !== "undefined") {
          return global2;
        }
        return {};
      })();
      function getWorkerBlob() {
        var URL2 = global2.URL || global2.webkitURL || null;
        var code = moduleFactory.toString();
        return Papa2.BLOB_URL || (Papa2.BLOB_URL = URL2.createObjectURL(new Blob(["var global = (function() { if (typeof self !== 'undefined') { return self; } if (typeof window !== 'undefined') { return window; } if (typeof global !== 'undefined') { return global; } return {}; })(); global.IS_PAPA_WORKER=true; ", "(", code, ")();"], { type: "text/javascript" })));
      }
      var IS_WORKER = !global2.document && !!global2.postMessage, IS_PAPA_WORKER = global2.IS_PAPA_WORKER || false;
      var workers = {}, workerIdCounter = 0;
      var Papa2 = {};
      Papa2.parse = CsvToJson;
      Papa2.unparse = JsonToCsv;
      Papa2.RECORD_SEP = String.fromCharCode(30);
      Papa2.UNIT_SEP = String.fromCharCode(31);
      Papa2.BYTE_ORDER_MARK = "\uFEFF";
      Papa2.BAD_DELIMITERS = ["\r", "\n", '"', Papa2.BYTE_ORDER_MARK];
      Papa2.WORKERS_SUPPORTED = !IS_WORKER && !!global2.Worker;
      Papa2.NODE_STREAM_INPUT = 1;
      Papa2.LocalChunkSize = 1024 * 1024 * 10;
      Papa2.RemoteChunkSize = 1024 * 1024 * 5;
      Papa2.DefaultDelimiter = ",";
      Papa2.Parser = Parser;
      Papa2.ParserHandle = ParserHandle;
      Papa2.NetworkStreamer = NetworkStreamer;
      Papa2.FileStreamer = FileStreamer;
      Papa2.StringStreamer = StringStreamer;
      Papa2.ReadableStreamStreamer = ReadableStreamStreamer;
      if (typeof PAPA_BROWSER_CONTEXT === "undefined") {
        Papa2.DuplexStreamStreamer = DuplexStreamStreamer;
      }
      if (global2.jQuery) {
        var $ = global2.jQuery;
        $.fn.parse = function(options) {
          var config2 = options.config || {};
          var queue = [];
          this.each(function(idx) {
            var supported = $(this).prop("tagName").toUpperCase() === "INPUT" && $(this).attr("type").toLowerCase() === "file" && global2.FileReader;
            if (!supported || !this.files || this.files.length === 0)
              return true;
            for (var i = 0; i < this.files.length; i++) {
              queue.push({
                file: this.files[i],
                inputElem: this,
                instanceConfig: $.extend({}, config2)
              });
            }
          });
          parseNextFile();
          return this;
          function parseNextFile() {
            if (queue.length === 0) {
              if (isFunction(options.complete))
                options.complete();
              return;
            }
            var f = queue[0];
            if (isFunction(options.before)) {
              var returned = options.before(f.file, f.inputElem);
              if (typeof returned === "object") {
                if (returned.action === "abort") {
                  error("AbortError", f.file, f.inputElem, returned.reason);
                  return;
                } else if (returned.action === "skip") {
                  fileComplete();
                  return;
                } else if (typeof returned.config === "object")
                  f.instanceConfig = $.extend(f.instanceConfig, returned.config);
              } else if (returned === "skip") {
                fileComplete();
                return;
              }
            }
            var userCompleteFunc = f.instanceConfig.complete;
            f.instanceConfig.complete = function(results) {
              if (isFunction(userCompleteFunc))
                userCompleteFunc(results, f.file, f.inputElem);
              fileComplete();
            };
            Papa2.parse(f.file, f.instanceConfig);
          }
          function error(name, file, elem, reason) {
            if (isFunction(options.error))
              options.error({ name }, file, elem, reason);
          }
          function fileComplete() {
            queue.splice(0, 1);
            parseNextFile();
          }
        };
      }
      if (IS_PAPA_WORKER) {
        global2.onmessage = workerThreadReceivedMessage;
      }
      function CsvToJson(_input, _config) {
        _config = _config || {};
        var dynamicTyping = _config.dynamicTyping || false;
        if (isFunction(dynamicTyping)) {
          _config.dynamicTypingFunction = dynamicTyping;
          dynamicTyping = {};
        }
        _config.dynamicTyping = dynamicTyping;
        _config.transform = isFunction(_config.transform) ? _config.transform : false;
        if (_config.worker && Papa2.WORKERS_SUPPORTED) {
          var w = newWorker();
          w.userStep = _config.step;
          w.userChunk = _config.chunk;
          w.userComplete = _config.complete;
          w.userError = _config.error;
          _config.step = isFunction(_config.step);
          _config.chunk = isFunction(_config.chunk);
          _config.complete = isFunction(_config.complete);
          _config.error = isFunction(_config.error);
          delete _config.worker;
          w.postMessage({
            input: _input,
            config: _config,
            workerId: w.id
          });
          return;
        }
        var streamer = null;
        if (_input === Papa2.NODE_STREAM_INPUT && typeof PAPA_BROWSER_CONTEXT === "undefined") {
          streamer = new DuplexStreamStreamer(_config);
          return streamer.getStream();
        } else if (typeof _input === "string") {
          _input = stripBom(_input);
          if (_config.download)
            streamer = new NetworkStreamer(_config);
          else
            streamer = new StringStreamer(_config);
        } else if (_input.readable === true && isFunction(_input.read) && isFunction(_input.on)) {
          streamer = new ReadableStreamStreamer(_config);
        } else if (global2.File && _input instanceof File || _input instanceof Object)
          streamer = new FileStreamer(_config);
        return streamer.stream(_input);
        function stripBom(string) {
          if (string.charCodeAt(0) === 65279) {
            return string.slice(1);
          }
          return string;
        }
      }
      function JsonToCsv(_input, _config) {
        var _quotes = false;
        var _writeHeader = true;
        var _delimiter = ",";
        var _newline = "\r\n";
        var _quoteChar = '"';
        var _escapedQuote = _quoteChar + _quoteChar;
        var _skipEmptyLines = false;
        var _columns = null;
        var _escapeFormulae = false;
        unpackConfig();
        var quoteCharRegex = new RegExp(escapeRegExp(_quoteChar), "g");
        if (typeof _input === "string")
          _input = JSON.parse(_input);
        if (Array.isArray(_input)) {
          if (!_input.length || Array.isArray(_input[0]))
            return serialize(null, _input, _skipEmptyLines);
          else if (typeof _input[0] === "object")
            return serialize(_columns || Object.keys(_input[0]), _input, _skipEmptyLines);
        } else if (typeof _input === "object") {
          if (typeof _input.data === "string")
            _input.data = JSON.parse(_input.data);
          if (Array.isArray(_input.data)) {
            if (!_input.fields)
              _input.fields = _input.meta && _input.meta.fields || _columns;
            if (!_input.fields)
              _input.fields = Array.isArray(_input.data[0]) ? _input.fields : typeof _input.data[0] === "object" ? Object.keys(_input.data[0]) : [];
            if (!Array.isArray(_input.data[0]) && typeof _input.data[0] !== "object")
              _input.data = [_input.data];
          }
          return serialize(_input.fields || [], _input.data || [], _skipEmptyLines);
        }
        throw new Error("Unable to serialize unrecognized input");
        function unpackConfig() {
          if (typeof _config !== "object")
            return;
          if (typeof _config.delimiter === "string" && !Papa2.BAD_DELIMITERS.filter(function(value) {
            return _config.delimiter.indexOf(value) !== -1;
          }).length) {
            _delimiter = _config.delimiter;
          }
          if (typeof _config.quotes === "boolean" || typeof _config.quotes === "function" || Array.isArray(_config.quotes))
            _quotes = _config.quotes;
          if (typeof _config.skipEmptyLines === "boolean" || typeof _config.skipEmptyLines === "string")
            _skipEmptyLines = _config.skipEmptyLines;
          if (typeof _config.newline === "string")
            _newline = _config.newline;
          if (typeof _config.quoteChar === "string")
            _quoteChar = _config.quoteChar;
          if (typeof _config.header === "boolean")
            _writeHeader = _config.header;
          if (Array.isArray(_config.columns)) {
            if (_config.columns.length === 0) throw new Error("Option columns is empty");
            _columns = _config.columns;
          }
          if (_config.escapeChar !== void 0) {
            _escapedQuote = _config.escapeChar + _quoteChar;
          }
          if (_config.escapeFormulae instanceof RegExp) {
            _escapeFormulae = _config.escapeFormulae;
          } else if (typeof _config.escapeFormulae === "boolean" && _config.escapeFormulae) {
            _escapeFormulae = /^[=+\-@\t\r].*$/;
          }
        }
        function serialize(fields, data, skipEmptyLines) {
          var csv = "";
          if (typeof fields === "string")
            fields = JSON.parse(fields);
          if (typeof data === "string")
            data = JSON.parse(data);
          var hasHeader = Array.isArray(fields) && fields.length > 0;
          var dataKeyedByField = !Array.isArray(data[0]);
          if (hasHeader && _writeHeader) {
            for (var i = 0; i < fields.length; i++) {
              if (i > 0)
                csv += _delimiter;
              csv += safe(fields[i], i);
            }
            if (data.length > 0)
              csv += _newline;
          }
          for (var row = 0; row < data.length; row++) {
            var maxCol = hasHeader ? fields.length : data[row].length;
            var emptyLine = false;
            var nullLine = hasHeader ? Object.keys(data[row]).length === 0 : data[row].length === 0;
            if (skipEmptyLines && !hasHeader) {
              emptyLine = skipEmptyLines === "greedy" ? data[row].join("").trim() === "" : data[row].length === 1 && data[row][0].length === 0;
            }
            if (skipEmptyLines === "greedy" && hasHeader) {
              var line = [];
              for (var c = 0; c < maxCol; c++) {
                var cx = dataKeyedByField ? fields[c] : c;
                line.push(data[row][cx]);
              }
              emptyLine = line.join("").trim() === "";
            }
            if (!emptyLine) {
              for (var col = 0; col < maxCol; col++) {
                if (col > 0 && !nullLine)
                  csv += _delimiter;
                var colIdx = hasHeader && dataKeyedByField ? fields[col] : col;
                csv += safe(data[row][colIdx], col);
              }
              if (row < data.length - 1 && (!skipEmptyLines || maxCol > 0 && !nullLine)) {
                csv += _newline;
              }
            }
          }
          return csv;
        }
        function safe(str, col) {
          if (typeof str === "undefined" || str === null)
            return "";
          if (str.constructor === Date)
            return JSON.stringify(str).slice(1, 25);
          var needsQuotes = false;
          if (_escapeFormulae && typeof str === "string" && _escapeFormulae.test(str)) {
            str = "'" + str;
            needsQuotes = true;
          }
          var escapedQuoteStr = str.toString().replace(quoteCharRegex, _escapedQuote);
          needsQuotes = needsQuotes || _quotes === true || typeof _quotes === "function" && _quotes(str, col) || Array.isArray(_quotes) && _quotes[col] || hasAny(escapedQuoteStr, Papa2.BAD_DELIMITERS) || escapedQuoteStr.indexOf(_delimiter) > -1 || escapedQuoteStr.charAt(0) === " " || escapedQuoteStr.charAt(escapedQuoteStr.length - 1) === " ";
          return needsQuotes ? _quoteChar + escapedQuoteStr + _quoteChar : escapedQuoteStr;
        }
        function hasAny(str, substrings) {
          for (var i = 0; i < substrings.length; i++)
            if (str.indexOf(substrings[i]) > -1)
              return true;
          return false;
        }
      }
      function ChunkStreamer(config2) {
        this._handle = null;
        this._finished = false;
        this._completed = false;
        this._halted = false;
        this._input = null;
        this._baseIndex = 0;
        this._partialLine = "";
        this._rowCount = 0;
        this._start = 0;
        this._nextChunk = null;
        this.isFirstChunk = true;
        this._completeResults = {
          data: [],
          errors: [],
          meta: {}
        };
        replaceConfig.call(this, config2);
        this.parseChunk = function(chunk, isFakeChunk) {
          const skipFirstNLines = parseInt(this._config.skipFirstNLines) || 0;
          if (this.isFirstChunk && skipFirstNLines > 0) {
            let _newline = this._config.newline;
            if (!_newline) {
              const quoteChar = this._config.quoteChar || '"';
              _newline = this._handle.guessLineEndings(chunk, quoteChar);
            }
            const splitChunk = chunk.split(_newline);
            chunk = [...splitChunk.slice(skipFirstNLines)].join(_newline);
          }
          if (this.isFirstChunk && isFunction(this._config.beforeFirstChunk)) {
            var modifiedChunk = this._config.beforeFirstChunk(chunk);
            if (modifiedChunk !== void 0)
              chunk = modifiedChunk;
          }
          this.isFirstChunk = false;
          this._halted = false;
          var aggregate = this._partialLine + chunk;
          this._partialLine = "";
          var results = this._handle.parse(aggregate, this._baseIndex, !this._finished);
          if (this._handle.paused() || this._handle.aborted()) {
            this._halted = true;
            return;
          }
          var lastIndex = results.meta.cursor;
          if (!this._finished) {
            this._partialLine = aggregate.substring(lastIndex - this._baseIndex);
            this._baseIndex = lastIndex;
          }
          if (results && results.data)
            this._rowCount += results.data.length;
          var finishedIncludingPreview = this._finished || this._config.preview && this._rowCount >= this._config.preview;
          if (IS_PAPA_WORKER) {
            global2.postMessage({
              results,
              workerId: Papa2.WORKER_ID,
              finished: finishedIncludingPreview
            });
          } else if (isFunction(this._config.chunk) && !isFakeChunk) {
            this._config.chunk(results, this._handle);
            if (this._handle.paused() || this._handle.aborted()) {
              this._halted = true;
              return;
            }
            results = void 0;
            this._completeResults = void 0;
          }
          if (!this._config.step && !this._config.chunk) {
            this._completeResults.data = this._completeResults.data.concat(results.data);
            this._completeResults.errors = this._completeResults.errors.concat(results.errors);
            this._completeResults.meta = results.meta;
          }
          if (!this._completed && finishedIncludingPreview && isFunction(this._config.complete) && (!results || !results.meta.aborted)) {
            this._config.complete(this._completeResults, this._input);
            this._completed = true;
          }
          if (!finishedIncludingPreview && (!results || !results.meta.paused))
            this._nextChunk();
          return results;
        };
        this._sendError = function(error) {
          if (isFunction(this._config.error))
            this._config.error(error);
          else if (IS_PAPA_WORKER && this._config.error) {
            global2.postMessage({
              workerId: Papa2.WORKER_ID,
              error,
              finished: false
            });
          }
        };
        function replaceConfig(config3) {
          var configCopy = copy(config3);
          configCopy.chunkSize = parseInt(configCopy.chunkSize);
          if (!config3.step && !config3.chunk)
            configCopy.chunkSize = null;
          this._handle = new ParserHandle(configCopy);
          this._handle.streamer = this;
          this._config = configCopy;
        }
      }
      function NetworkStreamer(config2) {
        config2 = config2 || {};
        if (!config2.chunkSize)
          config2.chunkSize = Papa2.RemoteChunkSize;
        ChunkStreamer.call(this, config2);
        var xhr;
        if (IS_WORKER) {
          this._nextChunk = function() {
            this._readChunk();
            this._chunkLoaded();
          };
        } else {
          this._nextChunk = function() {
            this._readChunk();
          };
        }
        this.stream = function(url) {
          this._input = url;
          this._nextChunk();
        };
        this._readChunk = function() {
          if (this._finished) {
            this._chunkLoaded();
            return;
          }
          xhr = new XMLHttpRequest();
          if (this._config.withCredentials) {
            xhr.withCredentials = this._config.withCredentials;
          }
          if (!IS_WORKER) {
            xhr.onload = bindFunction(this._chunkLoaded, this);
            xhr.onerror = bindFunction(this._chunkError, this);
          }
          xhr.open(this._config.downloadRequestBody ? "POST" : "GET", this._input, !IS_WORKER);
          if (this._config.downloadRequestHeaders) {
            var headers = this._config.downloadRequestHeaders;
            for (var headerName in headers) {
              xhr.setRequestHeader(headerName, headers[headerName]);
            }
          }
          if (this._config.chunkSize) {
            var end = this._start + this._config.chunkSize - 1;
            xhr.setRequestHeader("Range", "bytes=" + this._start + "-" + end);
          }
          try {
            xhr.send(this._config.downloadRequestBody);
          } catch (err) {
            this._chunkError(err.message);
          }
          if (IS_WORKER && xhr.status === 0)
            this._chunkError();
        };
        this._chunkLoaded = function() {
          if (xhr.readyState !== 4)
            return;
          if (xhr.status < 200 || xhr.status >= 400) {
            this._chunkError();
            return;
          }
          this._start += this._config.chunkSize ? this._config.chunkSize : xhr.responseText.length;
          this._finished = !this._config.chunkSize || this._start >= getFileSize(xhr);
          this.parseChunk(xhr.responseText);
        };
        this._chunkError = function(errorMessage) {
          var errorText = xhr.statusText || errorMessage;
          this._sendError(new Error(errorText));
        };
        function getFileSize(xhr2) {
          var contentRange = xhr2.getResponseHeader("Content-Range");
          if (contentRange === null) {
            return -1;
          }
          return parseInt(contentRange.substring(contentRange.lastIndexOf("/") + 1));
        }
      }
      NetworkStreamer.prototype = Object.create(ChunkStreamer.prototype);
      NetworkStreamer.prototype.constructor = NetworkStreamer;
      function FileStreamer(config2) {
        config2 = config2 || {};
        if (!config2.chunkSize)
          config2.chunkSize = Papa2.LocalChunkSize;
        ChunkStreamer.call(this, config2);
        var reader, slice;
        var usingAsyncReader = typeof FileReader !== "undefined";
        this.stream = function(file) {
          this._input = file;
          slice = file.slice || file.webkitSlice || file.mozSlice;
          if (usingAsyncReader) {
            reader = new FileReader();
            reader.onload = bindFunction(this._chunkLoaded, this);
            reader.onerror = bindFunction(this._chunkError, this);
          } else
            reader = new FileReaderSync();
          this._nextChunk();
        };
        this._nextChunk = function() {
          if (!this._finished && (!this._config.preview || this._rowCount < this._config.preview))
            this._readChunk();
        };
        this._readChunk = function() {
          var input = this._input;
          if (this._config.chunkSize) {
            var end = Math.min(this._start + this._config.chunkSize, this._input.size);
            input = slice.call(input, this._start, end);
          }
          var txt = reader.readAsText(input, this._config.encoding);
          if (!usingAsyncReader)
            this._chunkLoaded({ target: { result: txt } });
        };
        this._chunkLoaded = function(event) {
          this._start += this._config.chunkSize;
          this._finished = !this._config.chunkSize || this._start >= this._input.size;
          this.parseChunk(event.target.result);
        };
        this._chunkError = function() {
          this._sendError(reader.error);
        };
      }
      FileStreamer.prototype = Object.create(ChunkStreamer.prototype);
      FileStreamer.prototype.constructor = FileStreamer;
      function StringStreamer(config2) {
        config2 = config2 || {};
        ChunkStreamer.call(this, config2);
        var remaining;
        this.stream = function(s) {
          remaining = s;
          return this._nextChunk();
        };
        this._nextChunk = function() {
          if (this._finished) return;
          var size = this._config.chunkSize;
          var chunk;
          if (size) {
            chunk = remaining.substring(0, size);
            remaining = remaining.substring(size);
          } else {
            chunk = remaining;
            remaining = "";
          }
          this._finished = !remaining;
          return this.parseChunk(chunk);
        };
      }
      StringStreamer.prototype = Object.create(StringStreamer.prototype);
      StringStreamer.prototype.constructor = StringStreamer;
      function ReadableStreamStreamer(config2) {
        config2 = config2 || {};
        ChunkStreamer.call(this, config2);
        var queue = [];
        var parseOnData = true;
        var streamHasEnded = false;
        this.pause = function() {
          ChunkStreamer.prototype.pause.apply(this, arguments);
          this._input.pause();
        };
        this.resume = function() {
          ChunkStreamer.prototype.resume.apply(this, arguments);
          this._input.resume();
        };
        this.stream = function(stream) {
          this._input = stream;
          this._input.on("data", this._streamData);
          this._input.on("end", this._streamEnd);
          this._input.on("error", this._streamError);
        };
        this._checkIsFinished = function() {
          if (streamHasEnded && queue.length === 1) {
            this._finished = true;
          }
        };
        this._nextChunk = function() {
          this._checkIsFinished();
          if (queue.length) {
            this.parseChunk(queue.shift());
          } else {
            parseOnData = true;
          }
        };
        this._streamData = bindFunction(function(chunk) {
          try {
            queue.push(typeof chunk === "string" ? chunk : chunk.toString(this._config.encoding));
            if (parseOnData) {
              parseOnData = false;
              this._checkIsFinished();
              this.parseChunk(queue.shift());
            }
          } catch (error) {
            this._streamError(error);
          }
        }, this);
        this._streamError = bindFunction(function(error) {
          this._streamCleanUp();
          this._sendError(error);
        }, this);
        this._streamEnd = bindFunction(function() {
          this._streamCleanUp();
          streamHasEnded = true;
          this._streamData("");
        }, this);
        this._streamCleanUp = bindFunction(function() {
          this._input.removeListener("data", this._streamData);
          this._input.removeListener("end", this._streamEnd);
          this._input.removeListener("error", this._streamError);
        }, this);
      }
      ReadableStreamStreamer.prototype = Object.create(ChunkStreamer.prototype);
      ReadableStreamStreamer.prototype.constructor = ReadableStreamStreamer;
      function DuplexStreamStreamer(_config) {
        var Duplex = __require("stream").Duplex;
        var config2 = copy(_config);
        var parseOnWrite = true;
        var writeStreamHasFinished = false;
        var parseCallbackQueue = [];
        var stream = null;
        this._onCsvData = function(results) {
          var data = results.data;
          if (!stream.push(data) && !this._handle.paused()) {
            this._handle.pause();
          }
        };
        this._onCsvComplete = function() {
          stream.push(null);
        };
        config2.step = bindFunction(this._onCsvData, this);
        config2.complete = bindFunction(this._onCsvComplete, this);
        ChunkStreamer.call(this, config2);
        this._nextChunk = function() {
          if (writeStreamHasFinished && parseCallbackQueue.length === 1) {
            this._finished = true;
          }
          if (parseCallbackQueue.length) {
            parseCallbackQueue.shift()();
          } else {
            parseOnWrite = true;
          }
        };
        this._addToParseQueue = function(chunk, callback) {
          parseCallbackQueue.push(bindFunction(function() {
            this.parseChunk(typeof chunk === "string" ? chunk : chunk.toString(config2.encoding));
            if (isFunction(callback)) {
              return callback();
            }
          }, this));
          if (parseOnWrite) {
            parseOnWrite = false;
            this._nextChunk();
          }
        };
        this._onRead = function() {
          if (this._handle.paused()) {
            this._handle.resume();
          }
        };
        this._onWrite = function(chunk, encoding, callback) {
          this._addToParseQueue(chunk, callback);
        };
        this._onWriteComplete = function() {
          writeStreamHasFinished = true;
          this._addToParseQueue("");
        };
        this.getStream = function() {
          return stream;
        };
        stream = new Duplex({
          readableObjectMode: true,
          decodeStrings: false,
          read: bindFunction(this._onRead, this),
          write: bindFunction(this._onWrite, this)
        });
        stream.once("finish", bindFunction(this._onWriteComplete, this));
      }
      if (typeof PAPA_BROWSER_CONTEXT === "undefined") {
        DuplexStreamStreamer.prototype = Object.create(ChunkStreamer.prototype);
        DuplexStreamStreamer.prototype.constructor = DuplexStreamStreamer;
      }
      function ParserHandle(_config) {
        var MAX_FLOAT = Math.pow(2, 53);
        var MIN_FLOAT = -MAX_FLOAT;
        var FLOAT = /^\s*-?(\d+\.?|\.\d+|\d+\.\d+)([eE][-+]?\d+)?\s*$/;
        var ISO_DATE = /^((\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d\.\d+([+-][0-2]\d:[0-5]\d|Z))|(\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z))|(\d{4}-[01]\d-[0-3]\dT[0-2]\d:[0-5]\d([+-][0-2]\d:[0-5]\d|Z)))$/;
        var self2 = this;
        var _stepCounter = 0;
        var _rowCounter = 0;
        var _input;
        var _parser;
        var _paused = false;
        var _aborted = false;
        var _delimiterError;
        var _fields = [];
        var _results = {
          // The last results returned from the parser
          data: [],
          errors: [],
          meta: {}
        };
        if (isFunction(_config.step)) {
          var userStep = _config.step;
          _config.step = function(results) {
            _results = results;
            if (needsHeaderRow())
              processResults();
            else {
              processResults();
              if (_results.data.length === 0)
                return;
              _stepCounter += results.data.length;
              if (_config.preview && _stepCounter > _config.preview)
                _parser.abort();
              else {
                _results.data = _results.data[0];
                userStep(_results, self2);
              }
            }
          };
        }
        this.parse = function(input, baseIndex, ignoreLastRow) {
          var quoteChar = _config.quoteChar || '"';
          if (!_config.newline)
            _config.newline = this.guessLineEndings(input, quoteChar);
          _delimiterError = false;
          if (!_config.delimiter) {
            var delimGuess = guessDelimiter(input, _config.newline, _config.skipEmptyLines, _config.comments, _config.delimitersToGuess);
            if (delimGuess.successful)
              _config.delimiter = delimGuess.bestDelimiter;
            else {
              _delimiterError = true;
              _config.delimiter = Papa2.DefaultDelimiter;
            }
            _results.meta.delimiter = _config.delimiter;
          } else if (isFunction(_config.delimiter)) {
            _config.delimiter = _config.delimiter(input);
            _results.meta.delimiter = _config.delimiter;
          }
          var parserConfig = copy(_config);
          if (_config.preview && _config.header)
            parserConfig.preview++;
          _input = input;
          _parser = new Parser(parserConfig);
          _results = _parser.parse(_input, baseIndex, ignoreLastRow);
          processResults();
          return _paused ? { meta: { paused: true } } : _results || { meta: { paused: false } };
        };
        this.paused = function() {
          return _paused;
        };
        this.pause = function() {
          _paused = true;
          _parser.abort();
          _input = isFunction(_config.chunk) ? "" : _input.substring(_parser.getCharIndex());
        };
        this.resume = function() {
          if (self2.streamer._halted) {
            _paused = false;
            self2.streamer.parseChunk(_input, true);
          } else {
            setTimeout(self2.resume, 3);
          }
        };
        this.aborted = function() {
          return _aborted;
        };
        this.abort = function() {
          _aborted = true;
          _parser.abort();
          _results.meta.aborted = true;
          if (isFunction(_config.complete))
            _config.complete(_results);
          _input = "";
        };
        this.guessLineEndings = function(input, quoteChar) {
          input = input.substring(0, 1024 * 1024);
          var re = new RegExp(escapeRegExp(quoteChar) + "([^]*?)" + escapeRegExp(quoteChar), "gm");
          input = input.replace(re, "");
          var r = input.split("\r");
          var n = input.split("\n");
          var nAppearsFirst = n.length > 1 && n[0].length < r[0].length;
          if (r.length === 1 || nAppearsFirst)
            return "\n";
          var numWithN = 0;
          for (var i = 0; i < r.length; i++) {
            if (r[i][0] === "\n")
              numWithN++;
          }
          return numWithN >= r.length / 2 ? "\r\n" : "\r";
        };
        function testEmptyLine(s) {
          return _config.skipEmptyLines === "greedy" ? s.join("").trim() === "" : s.length === 1 && s[0].length === 0;
        }
        function testFloat(s) {
          if (FLOAT.test(s)) {
            var floatValue = parseFloat(s);
            if (floatValue > MIN_FLOAT && floatValue < MAX_FLOAT) {
              return true;
            }
          }
          return false;
        }
        function processResults() {
          if (_results && _delimiterError) {
            addError("Delimiter", "UndetectableDelimiter", "Unable to auto-detect delimiting character; defaulted to '" + Papa2.DefaultDelimiter + "'");
            _delimiterError = false;
          }
          if (_config.skipEmptyLines) {
            _results.data = _results.data.filter(function(d) {
              return !testEmptyLine(d);
            });
          }
          if (needsHeaderRow())
            fillHeaderFields();
          return applyHeaderAndDynamicTypingAndTransformation();
        }
        function needsHeaderRow() {
          return _config.header && _fields.length === 0;
        }
        function fillHeaderFields() {
          if (!_results)
            return;
          function addHeader(header, i2) {
            if (isFunction(_config.transformHeader))
              header = _config.transformHeader(header, i2);
            _fields.push(header);
          }
          if (Array.isArray(_results.data[0])) {
            for (var i = 0; needsHeaderRow() && i < _results.data.length; i++)
              _results.data[i].forEach(addHeader);
            _results.data.splice(0, 1);
          } else
            _results.data.forEach(addHeader);
        }
        function shouldApplyDynamicTyping(field) {
          if (_config.dynamicTypingFunction && _config.dynamicTyping[field] === void 0) {
            _config.dynamicTyping[field] = _config.dynamicTypingFunction(field);
          }
          return (_config.dynamicTyping[field] || _config.dynamicTyping) === true;
        }
        function parseDynamic(field, value) {
          if (shouldApplyDynamicTyping(field)) {
            if (value === "true" || value === "TRUE")
              return true;
            else if (value === "false" || value === "FALSE")
              return false;
            else if (testFloat(value))
              return parseFloat(value);
            else if (ISO_DATE.test(value))
              return new Date(value);
            else
              return value === "" ? null : value;
          }
          return value;
        }
        function applyHeaderAndDynamicTypingAndTransformation() {
          if (!_results || !_config.header && !_config.dynamicTyping && !_config.transform)
            return _results;
          function processRow(rowSource, i) {
            var row = _config.header ? {} : [];
            var j;
            for (j = 0; j < rowSource.length; j++) {
              var field = j;
              var value = rowSource[j];
              if (_config.header)
                field = j >= _fields.length ? "__parsed_extra" : _fields[j];
              if (_config.transform)
                value = _config.transform(value, field);
              value = parseDynamic(field, value);
              if (field === "__parsed_extra") {
                row[field] = row[field] || [];
                row[field].push(value);
              } else
                row[field] = value;
            }
            if (_config.header) {
              if (j > _fields.length)
                addError("FieldMismatch", "TooManyFields", "Too many fields: expected " + _fields.length + " fields but parsed " + j, _rowCounter + i);
              else if (j < _fields.length)
                addError("FieldMismatch", "TooFewFields", "Too few fields: expected " + _fields.length + " fields but parsed " + j, _rowCounter + i);
            }
            return row;
          }
          var incrementBy = 1;
          if (!_results.data.length || Array.isArray(_results.data[0])) {
            _results.data = _results.data.map(processRow);
            incrementBy = _results.data.length;
          } else
            _results.data = processRow(_results.data, 0);
          if (_config.header && _results.meta)
            _results.meta.fields = _fields;
          _rowCounter += incrementBy;
          return _results;
        }
        function guessDelimiter(input, newline, skipEmptyLines, comments, delimitersToGuess) {
          var bestDelim, bestDelta, fieldCountPrevRow, maxFieldCount;
          delimitersToGuess = delimitersToGuess || [",", "	", "|", ";", Papa2.RECORD_SEP, Papa2.UNIT_SEP];
          for (var i = 0; i < delimitersToGuess.length; i++) {
            var delim = delimitersToGuess[i];
            var delta = 0, avgFieldCount = 0, emptyLinesCount = 0;
            fieldCountPrevRow = void 0;
            var preview = new Parser({
              comments,
              delimiter: delim,
              newline,
              preview: 10
            }).parse(input);
            for (var j = 0; j < preview.data.length; j++) {
              if (skipEmptyLines && testEmptyLine(preview.data[j])) {
                emptyLinesCount++;
                continue;
              }
              var fieldCount = preview.data[j].length;
              avgFieldCount += fieldCount;
              if (typeof fieldCountPrevRow === "undefined") {
                fieldCountPrevRow = fieldCount;
                continue;
              } else if (fieldCount > 0) {
                delta += Math.abs(fieldCount - fieldCountPrevRow);
                fieldCountPrevRow = fieldCount;
              }
            }
            if (preview.data.length > 0)
              avgFieldCount /= preview.data.length - emptyLinesCount;
            if ((typeof bestDelta === "undefined" || delta <= bestDelta) && (typeof maxFieldCount === "undefined" || avgFieldCount > maxFieldCount) && avgFieldCount > 1.99) {
              bestDelta = delta;
              bestDelim = delim;
              maxFieldCount = avgFieldCount;
            }
          }
          _config.delimiter = bestDelim;
          return {
            successful: !!bestDelim,
            bestDelimiter: bestDelim
          };
        }
        function addError(type, code, msg, row) {
          var error = {
            type,
            code,
            message: msg
          };
          if (row !== void 0) {
            error.row = row;
          }
          _results.errors.push(error);
        }
      }
      function escapeRegExp(string) {
        return string.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      }
      function Parser(config2) {
        config2 = config2 || {};
        var delim = config2.delimiter;
        var newline = config2.newline;
        var comments = config2.comments;
        var step = config2.step;
        var preview = config2.preview;
        var fastMode = config2.fastMode;
        var quoteChar;
        var renamedHeaders = null;
        var headerParsed = false;
        if (config2.quoteChar === void 0 || config2.quoteChar === null) {
          quoteChar = '"';
        } else {
          quoteChar = config2.quoteChar;
        }
        var escapeChar = quoteChar;
        if (config2.escapeChar !== void 0) {
          escapeChar = config2.escapeChar;
        }
        if (typeof delim !== "string" || Papa2.BAD_DELIMITERS.indexOf(delim) > -1)
          delim = ",";
        if (comments === delim)
          throw new Error("Comment character same as delimiter");
        else if (comments === true)
          comments = "#";
        else if (typeof comments !== "string" || Papa2.BAD_DELIMITERS.indexOf(comments) > -1)
          comments = false;
        if (newline !== "\n" && newline !== "\r" && newline !== "\r\n")
          newline = "\n";
        var cursor = 0;
        var aborted = false;
        this.parse = function(input, baseIndex, ignoreLastRow) {
          if (typeof input !== "string")
            throw new Error("Input must be a string");
          var inputLen = input.length, delimLen = delim.length, newlineLen = newline.length, commentsLen = comments.length;
          var stepIsFunction = isFunction(step);
          cursor = 0;
          var data = [], errors = [], row = [], lastCursor = 0;
          if (!input)
            return returnable();
          if (fastMode || fastMode !== false && input.indexOf(quoteChar) === -1) {
            var rows = input.split(newline);
            for (var i = 0; i < rows.length; i++) {
              row = rows[i];
              cursor += row.length;
              if (i !== rows.length - 1)
                cursor += newline.length;
              else if (ignoreLastRow)
                return returnable();
              if (comments && row.substring(0, commentsLen) === comments)
                continue;
              if (stepIsFunction) {
                data = [];
                pushRow(row.split(delim));
                doStep();
                if (aborted)
                  return returnable();
              } else
                pushRow(row.split(delim));
              if (preview && i >= preview) {
                data = data.slice(0, preview);
                return returnable(true);
              }
            }
            return returnable();
          }
          var nextDelim = input.indexOf(delim, cursor);
          var nextNewline = input.indexOf(newline, cursor);
          var quoteCharRegex = new RegExp(escapeRegExp(escapeChar) + escapeRegExp(quoteChar), "g");
          var quoteSearch = input.indexOf(quoteChar, cursor);
          for (; ; ) {
            if (input[cursor] === quoteChar) {
              quoteSearch = cursor;
              cursor++;
              for (; ; ) {
                quoteSearch = input.indexOf(quoteChar, quoteSearch + 1);
                if (quoteSearch === -1) {
                  if (!ignoreLastRow) {
                    errors.push({
                      type: "Quotes",
                      code: "MissingQuotes",
                      message: "Quoted field unterminated",
                      row: data.length,
                      // row has yet to be inserted
                      index: cursor
                    });
                  }
                  return finish();
                }
                if (quoteSearch === inputLen - 1) {
                  var value = input.substring(cursor, quoteSearch).replace(quoteCharRegex, quoteChar);
                  return finish(value);
                }
                if (quoteChar === escapeChar && input[quoteSearch + 1] === escapeChar) {
                  quoteSearch++;
                  continue;
                }
                if (quoteChar !== escapeChar && quoteSearch !== 0 && input[quoteSearch - 1] === escapeChar) {
                  continue;
                }
                if (nextDelim !== -1 && nextDelim < quoteSearch + 1) {
                  nextDelim = input.indexOf(delim, quoteSearch + 1);
                }
                if (nextNewline !== -1 && nextNewline < quoteSearch + 1) {
                  nextNewline = input.indexOf(newline, quoteSearch + 1);
                }
                var checkUpTo = nextNewline === -1 ? nextDelim : Math.min(nextDelim, nextNewline);
                var spacesBetweenQuoteAndDelimiter = extraSpaces(checkUpTo);
                if (input.substr(quoteSearch + 1 + spacesBetweenQuoteAndDelimiter, delimLen) === delim) {
                  row.push(input.substring(cursor, quoteSearch).replace(quoteCharRegex, quoteChar));
                  cursor = quoteSearch + 1 + spacesBetweenQuoteAndDelimiter + delimLen;
                  if (input[quoteSearch + 1 + spacesBetweenQuoteAndDelimiter + delimLen] !== quoteChar) {
                    quoteSearch = input.indexOf(quoteChar, cursor);
                  }
                  nextDelim = input.indexOf(delim, cursor);
                  nextNewline = input.indexOf(newline, cursor);
                  break;
                }
                var spacesBetweenQuoteAndNewLine = extraSpaces(nextNewline);
                if (input.substring(quoteSearch + 1 + spacesBetweenQuoteAndNewLine, quoteSearch + 1 + spacesBetweenQuoteAndNewLine + newlineLen) === newline) {
                  row.push(input.substring(cursor, quoteSearch).replace(quoteCharRegex, quoteChar));
                  saveRow(quoteSearch + 1 + spacesBetweenQuoteAndNewLine + newlineLen);
                  nextDelim = input.indexOf(delim, cursor);
                  quoteSearch = input.indexOf(quoteChar, cursor);
                  if (stepIsFunction) {
                    doStep();
                    if (aborted)
                      return returnable();
                  }
                  if (preview && data.length >= preview)
                    return returnable(true);
                  break;
                }
                errors.push({
                  type: "Quotes",
                  code: "InvalidQuotes",
                  message: "Trailing quote on quoted field is malformed",
                  row: data.length,
                  // row has yet to be inserted
                  index: cursor
                });
                quoteSearch++;
                continue;
              }
              continue;
            }
            if (comments && row.length === 0 && input.substring(cursor, cursor + commentsLen) === comments) {
              if (nextNewline === -1)
                return returnable();
              cursor = nextNewline + newlineLen;
              nextNewline = input.indexOf(newline, cursor);
              nextDelim = input.indexOf(delim, cursor);
              continue;
            }
            if (nextDelim !== -1 && (nextDelim < nextNewline || nextNewline === -1)) {
              row.push(input.substring(cursor, nextDelim));
              cursor = nextDelim + delimLen;
              nextDelim = input.indexOf(delim, cursor);
              continue;
            }
            if (nextNewline !== -1) {
              row.push(input.substring(cursor, nextNewline));
              saveRow(nextNewline + newlineLen);
              if (stepIsFunction) {
                doStep();
                if (aborted)
                  return returnable();
              }
              if (preview && data.length >= preview)
                return returnable(true);
              continue;
            }
            break;
          }
          return finish();
          function pushRow(row2) {
            data.push(row2);
            lastCursor = cursor;
          }
          function extraSpaces(index) {
            var spaceLength = 0;
            if (index !== -1) {
              var textBetweenClosingQuoteAndIndex = input.substring(quoteSearch + 1, index);
              if (textBetweenClosingQuoteAndIndex && textBetweenClosingQuoteAndIndex.trim() === "") {
                spaceLength = textBetweenClosingQuoteAndIndex.length;
              }
            }
            return spaceLength;
          }
          function finish(value2) {
            if (ignoreLastRow)
              return returnable();
            if (typeof value2 === "undefined")
              value2 = input.substring(cursor);
            row.push(value2);
            cursor = inputLen;
            pushRow(row);
            if (stepIsFunction)
              doStep();
            return returnable();
          }
          function saveRow(newCursor) {
            cursor = newCursor;
            pushRow(row);
            row = [];
            nextNewline = input.indexOf(newline, cursor);
          }
          function returnable(stopped) {
            if (config2.header && !baseIndex && data.length && !headerParsed) {
              const result = data[0];
              const headerCount = /* @__PURE__ */ Object.create(null);
              const usedHeaders = new Set(result);
              let duplicateHeaders = false;
              for (let i2 = 0; i2 < result.length; i2++) {
                let header = result[i2];
                if (isFunction(config2.transformHeader))
                  header = config2.transformHeader(header, i2);
                if (!headerCount[header]) {
                  headerCount[header] = 1;
                  result[i2] = header;
                } else {
                  let newHeader;
                  let suffixCount = headerCount[header];
                  do {
                    newHeader = `${header}_${suffixCount}`;
                    suffixCount++;
                  } while (usedHeaders.has(newHeader));
                  usedHeaders.add(newHeader);
                  result[i2] = newHeader;
                  headerCount[header]++;
                  duplicateHeaders = true;
                  if (renamedHeaders === null) {
                    renamedHeaders = {};
                  }
                  renamedHeaders[newHeader] = header;
                }
                usedHeaders.add(header);
              }
              if (duplicateHeaders) {
                console.warn("Duplicate headers found and renamed.");
              }
              headerParsed = true;
            }
            return {
              data,
              errors,
              meta: {
                delimiter: delim,
                linebreak: newline,
                aborted,
                truncated: !!stopped,
                cursor: lastCursor + (baseIndex || 0),
                renamedHeaders
              }
            };
          }
          function doStep() {
            step(returnable());
            data = [];
            errors = [];
          }
        };
        this.abort = function() {
          aborted = true;
        };
        this.getCharIndex = function() {
          return cursor;
        };
      }
      function newWorker() {
        if (!Papa2.WORKERS_SUPPORTED)
          return false;
        var workerUrl = getWorkerBlob();
        var w = new global2.Worker(workerUrl);
        w.onmessage = mainThreadReceivedMessage;
        w.id = workerIdCounter++;
        workers[w.id] = w;
        return w;
      }
      function mainThreadReceivedMessage(e) {
        var msg = e.data;
        var worker = workers[msg.workerId];
        var aborted = false;
        if (msg.error)
          worker.userError(msg.error, msg.file);
        else if (msg.results && msg.results.data) {
          var abort = function() {
            aborted = true;
            completeWorker(msg.workerId, { data: [], errors: [], meta: { aborted: true } });
          };
          var handle = {
            abort,
            pause: notImplemented,
            resume: notImplemented
          };
          if (isFunction(worker.userStep)) {
            for (var i = 0; i < msg.results.data.length; i++) {
              worker.userStep({
                data: msg.results.data[i],
                errors: msg.results.errors,
                meta: msg.results.meta
              }, handle);
              if (aborted)
                break;
            }
            delete msg.results;
          } else if (isFunction(worker.userChunk)) {
            worker.userChunk(msg.results, handle, msg.file);
            delete msg.results;
          }
        }
        if (msg.finished && !aborted)
          completeWorker(msg.workerId, msg.results);
      }
      function completeWorker(workerId, results) {
        var worker = workers[workerId];
        if (isFunction(worker.userComplete))
          worker.userComplete(results);
        worker.terminate();
        delete workers[workerId];
      }
      function notImplemented() {
        throw new Error("Not implemented.");
      }
      function workerThreadReceivedMessage(e) {
        var msg = e.data;
        if (typeof Papa2.WORKER_ID === "undefined" && msg)
          Papa2.WORKER_ID = msg.workerId;
        if (typeof msg.input === "string") {
          global2.postMessage({
            workerId: Papa2.WORKER_ID,
            results: Papa2.parse(msg.input, msg.config),
            finished: true
          });
        } else if (global2.File && msg.input instanceof File || msg.input instanceof Object) {
          var results = Papa2.parse(msg.input, msg.config);
          if (results)
            global2.postMessage({
              workerId: Papa2.WORKER_ID,
              results,
              finished: true
            });
        }
      }
      function copy(obj) {
        if (typeof obj !== "object" || obj === null)
          return obj;
        var cpy = Array.isArray(obj) ? [] : {};
        for (var key in obj)
          cpy[key] = copy(obj[key]);
        return cpy;
      }
      function bindFunction(f, self2) {
        return function() {
          f.apply(self2, arguments);
        };
      }
      function isFunction(func) {
        return typeof func === "function";
      }
      return Papa2;
    });
  }
});

// server/router.ts
function createRouter(allRoutes2) {
  const staticTable = /* @__PURE__ */ new Map();
  const dynamicRoutes = [];
  for (const route of allRoutes2) {
    if (route.path.includes("{")) {
      const parts = route.path.split("/").filter(Boolean);
      dynamicRoutes.push({
        method: route.method,
        segmentCount: parts.length,
        segments: parts.map((p) => p.startsWith("{") && p.endsWith("}") ? null : p),
        handler: route.handler
      });
    } else {
      const key = `${route.method} ${route.path}`;
      staticTable.set(key, route.handler);
    }
  }
  return {
    match(req) {
      const url = new URL(req.url);
      const pathname = url.pathname.length > 1 && url.pathname.endsWith("/") ? url.pathname.slice(0, -1) : url.pathname;
      const key = `${req.method} ${pathname}`;
      const staticHandler = staticTable.get(key);
      if (staticHandler) return staticHandler;
      const parts = pathname.split("/").filter(Boolean);
      for (const route of dynamicRoutes) {
        if (route.method !== req.method) continue;
        if (route.segmentCount !== parts.length) continue;
        let matched = true;
        for (let i = 0; i < route.segmentCount; i++) {
          if (route.segments[i] !== null && route.segments[i] !== parts[i]) {
            matched = false;
            break;
          }
        }
        if (matched) return route.handler;
      }
      return null;
    }
  };
}

// server/cors.ts
var PRODUCTION_PATTERNS = [
  /^https:\/\/(.*\.)?worldmonitor\.app$/,
  /^https:\/\/worldmonitor-[a-z0-9-]+-elie-[a-z0-9]+\.vercel\.app$/,
  /^https?:\/\/tauri\.localhost(:\d+)?$/,
  /^https?:\/\/[a-z0-9-]+\.tauri\.localhost(:\d+)?$/i,
  /^tauri:\/\/localhost$/,
  /^asset:\/\/localhost$/
];
var DEV_PATTERNS = [
  /^https?:\/\/localhost(:\d+)?$/,
  /^https?:\/\/127\.0\.0\.1(:\d+)?$/
];
var ALLOWED_ORIGIN_PATTERNS = process.env.NODE_ENV === "production" ? PRODUCTION_PATTERNS : [...PRODUCTION_PATTERNS, ...DEV_PATTERNS];
function isAllowedOrigin(origin) {
  return Boolean(origin) && ALLOWED_ORIGIN_PATTERNS.some((pattern) => pattern.test(origin));
}
function getCorsHeaders(req) {
  const origin = req.headers.get("origin") || "";
  const allowOrigin = isAllowedOrigin(origin) ? origin : "https://worldmonitor.app";
  return {
    "Access-Control-Allow-Origin": allowOrigin,
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization, X-WorldMonitor-Key",
    "Access-Control-Max-Age": "86400",
    "Vary": "Origin"
  };
}
function isDisallowedOrigin(req) {
  const origin = req.headers.get("origin");
  if (!origin) return false;
  return !isAllowedOrigin(origin);
}

// api/_api-key.js
var DESKTOP_ORIGIN_PATTERNS = [
  /^https?:\/\/tauri\.localhost(:\d+)?$/,
  /^https?:\/\/[a-z0-9-]+\.tauri\.localhost(:\d+)?$/i,
  /^tauri:\/\/localhost$/,
  /^asset:\/\/localhost$/
];
var BROWSER_ORIGIN_PATTERNS = [
  /^https:\/\/(.*\.)?worldmonitor\.app$/,
  /^https:\/\/worldmonitor-[a-z0-9-]+\.vercel\.app$/,
  ...process.env.NODE_ENV === "production" ? [] : [
    /^https?:\/\/localhost(:\d+)?$/,
    /^https?:\/\/127\.0\.0\.1(:\d+)?$/
  ]
];
function isDesktopOrigin(origin) {
  return Boolean(origin) && DESKTOP_ORIGIN_PATTERNS.some((p) => p.test(origin));
}
function isTrustedBrowserOrigin(origin) {
  return Boolean(origin) && BROWSER_ORIGIN_PATTERNS.some((p) => p.test(origin));
}
function extractOriginFromReferer(referer) {
  if (!referer) return "";
  try {
    return new URL(referer).origin;
  } catch {
    return "";
  }
}
function validateApiKey(req) {
  const key = req.headers.get("X-WorldMonitor-Key");
  const origin = req.headers.get("Origin") || extractOriginFromReferer(req.headers.get("Referer")) || "";
  if (isDesktopOrigin(origin)) {
    if (!key) return { valid: false, required: true, error: "API key required for desktop access" };
    const validKeys = (process.env.WORLDMONITOR_VALID_KEYS || "").split(",").filter(Boolean);
    if (!validKeys.includes(key)) return { valid: false, required: true, error: "Invalid API key" };
    return { valid: true, required: true };
  }
  if (isTrustedBrowserOrigin(origin)) {
    if (key) {
      const validKeys = (process.env.WORLDMONITOR_VALID_KEYS || "").split(",").filter(Boolean);
      if (!validKeys.includes(key)) return { valid: false, required: true, error: "Invalid API key" };
    }
    return { valid: true, required: false };
  }
  if (key) {
    const validKeys = (process.env.WORLDMONITOR_VALID_KEYS || "").split(",").filter(Boolean);
    if (!validKeys.includes(key)) return { valid: false, required: true, error: "Invalid API key" };
    return { valid: true, required: true };
  }
  return { valid: false, required: true, error: "API key required" };
}

// server/error-mapper.ts
function isNetworkError(error) {
  if (!(error instanceof TypeError)) return false;
  const msg = error.message.toLowerCase();
  return msg.includes("fetch") || msg.includes("network") || msg.includes("connect") || msg.includes("econnrefused") || msg.includes("enotfound") || msg.includes("socket");
}
function mapErrorToResponse(error, _req) {
  if (error instanceof Error && "statusCode" in error) {
    const statusCode = error.statusCode;
    const message = statusCode >= 400 && statusCode < 500 ? error.message : "Internal server error";
    const body = { message };
    if (statusCode === 429 && "retryAfter" in error) {
      body.retryAfter = error.retryAfter;
    }
    if (statusCode >= 500) {
      const apiBody = "body" in error ? String(error.body).slice(0, 500) : "";
      console.error(`[error-mapper] ${statusCode}:`, error.message, apiBody ? `| body: ${apiBody}` : "");
    }
    return new Response(JSON.stringify(body), {
      status: statusCode,
      headers: { "Content-Type": "application/json" }
    });
  }
  if (isNetworkError(error)) {
    return new Response(JSON.stringify({ message: "Upstream unavailable" }), {
      status: 502,
      headers: { "Content-Type": "application/json" }
    });
  }
  console.error("[error-mapper] Unhandled error:", error instanceof Error ? error.message : error);
  return new Response(JSON.stringify({ message: "Internal server error" }), {
    status: 500,
    headers: { "Content-Type": "application/json" }
  });
}

// server/_shared/rate-limit.ts
var import_ratelimit = __toESM(require_dist2(), 1);

// node_modules/uncrypto/dist/crypto.node.mjs
import nodeCrypto from "node:crypto";
var subtle = nodeCrypto.webcrypto?.subtle || {};

// node_modules/@upstash/redis/chunk-LLI2WIYN.mjs
var __defProp2 = Object.defineProperty;
var __export = (target, all) => {
  for (var name in all)
    __defProp2(target, name, { get: all[name], enumerable: true });
};
var error_exports = {};
__export(error_exports, {
  UpstashError: () => UpstashError,
  UpstashJSONParseError: () => UpstashJSONParseError,
  UrlError: () => UrlError
});
var UpstashError = class extends Error {
  constructor(message, options) {
    super(message, options);
    this.name = "UpstashError";
  }
};
var UrlError = class extends Error {
  constructor(url) {
    super(
      `Upstash Redis client was passed an invalid URL. You should pass a URL starting with https. Received: "${url}". `
    );
    this.name = "UrlError";
  }
};
var UpstashJSONParseError = class extends UpstashError {
  constructor(body, options) {
    const truncatedBody = body.length > 200 ? body.slice(0, 200) + "..." : body;
    super(`Unable to parse response body: ${truncatedBody}`, options);
    this.name = "UpstashJSONParseError";
  }
};
function parseRecursive(obj) {
  const parsed = Array.isArray(obj) ? obj.map((o) => {
    try {
      return parseRecursive(o);
    } catch {
      return o;
    }
  }) : JSON.parse(obj);
  if (typeof parsed === "number" && parsed.toString() !== obj) {
    return obj;
  }
  return parsed;
}
function parseResponse(result) {
  try {
    return parseRecursive(result);
  } catch {
    return result;
  }
}
function deserializeScanResponse(result) {
  return [result[0], ...parseResponse(result.slice(1))];
}
function deserializeScanWithTypesResponse(result) {
  const [cursor, keys] = result;
  const parsedKeys = [];
  for (let i = 0; i < keys.length; i += 2) {
    parsedKeys.push({ key: keys[i], type: keys[i + 1] });
  }
  return [cursor, parsedKeys];
}
function mergeHeaders(...headers) {
  const merged = {};
  for (const header of headers) {
    if (!header) continue;
    for (const [key, value] of Object.entries(header)) {
      if (value !== void 0 && value !== null) {
        merged[key] = value;
      }
    }
  }
  return merged;
}
function kvArrayToObject(v) {
  if (typeof v === "object" && v !== null && !Array.isArray(v)) return v;
  if (!Array.isArray(v)) return {};
  const obj = {};
  for (let i = 0; i < v.length; i += 2) {
    if (typeof v[i] === "string") obj[v[i]] = v[i + 1];
  }
  return obj;
}
var MAX_BUFFER_SIZE = 1024 * 1024;
var HttpClient = class {
  baseUrl;
  headers;
  options;
  readYourWrites;
  upstashSyncToken = "";
  hasCredentials;
  retry;
  constructor(config2) {
    this.options = {
      backend: config2.options?.backend,
      agent: config2.agent,
      responseEncoding: config2.responseEncoding ?? "base64",
      // default to base64
      cache: config2.cache,
      signal: config2.signal,
      keepAlive: config2.keepAlive ?? true
    };
    this.upstashSyncToken = "";
    this.readYourWrites = config2.readYourWrites ?? true;
    this.baseUrl = (config2.baseUrl || "").replace(/\/$/, "");
    const urlRegex = /^https?:\/\/[^\s#$./?].\S*$/;
    if (this.baseUrl && !urlRegex.test(this.baseUrl)) {
      throw new UrlError(this.baseUrl);
    }
    this.headers = {
      "Content-Type": "application/json",
      ...config2.headers
    };
    this.hasCredentials = Boolean(this.baseUrl && this.headers.authorization.split(" ")[1]);
    if (this.options.responseEncoding === "base64") {
      this.headers["Upstash-Encoding"] = "base64";
    }
    this.retry = typeof config2.retry === "boolean" && !config2.retry ? {
      attempts: 1,
      backoff: () => 0
    } : {
      attempts: config2.retry?.retries ?? 5,
      backoff: config2.retry?.backoff ?? ((retryCount) => Math.exp(retryCount) * 50)
    };
  }
  mergeTelemetry(telemetry) {
    this.headers = merge(this.headers, "Upstash-Telemetry-Runtime", telemetry.runtime);
    this.headers = merge(this.headers, "Upstash-Telemetry-Platform", telemetry.platform);
    this.headers = merge(this.headers, "Upstash-Telemetry-Sdk", telemetry.sdk);
  }
  async request(req) {
    const requestHeaders = mergeHeaders(this.headers, req.headers ?? {});
    const requestUrl = [this.baseUrl, ...req.path ?? []].join("/");
    const isEventStream = requestHeaders.Accept === "text/event-stream";
    const signal = req.signal ?? this.options.signal;
    const isSignalFunction = typeof signal === "function";
    const requestOptions = {
      //@ts-expect-error this should throw due to bun regression
      cache: this.options.cache,
      method: "POST",
      headers: requestHeaders,
      body: JSON.stringify(req.body),
      keepalive: this.options.keepAlive,
      agent: this.options.agent,
      signal: isSignalFunction ? signal() : signal,
      /**
       * Fastly specific
       */
      backend: this.options.backend
    };
    if (!this.hasCredentials) {
      console.warn(
        "[Upstash Redis] Redis client was initialized without url or token. Failed to execute command."
      );
    }
    if (this.readYourWrites) {
      const newHeader = this.upstashSyncToken;
      this.headers["upstash-sync-token"] = newHeader;
    }
    let res = null;
    let error = null;
    for (let i = 0; i <= this.retry.attempts; i++) {
      try {
        res = await fetch(requestUrl, requestOptions);
        break;
      } catch (error_) {
        if (requestOptions.signal?.aborted && isSignalFunction) {
          throw error_;
        } else if (requestOptions.signal?.aborted) {
          const myBlob = new Blob([
            JSON.stringify({ result: requestOptions.signal.reason ?? "Aborted" })
          ]);
          const myOptions = {
            status: 200,
            statusText: requestOptions.signal.reason ?? "Aborted"
          };
          res = new Response(myBlob, myOptions);
          break;
        }
        error = error_;
        if (i < this.retry.attempts) {
          await new Promise((r) => setTimeout(r, this.retry.backoff(i)));
        }
      }
    }
    if (!res) {
      throw error ?? new Error("Exhausted all retries");
    }
    if (!res.ok) {
      let body2;
      const rawBody2 = await res.text();
      try {
        body2 = JSON.parse(rawBody2);
      } catch (error2) {
        throw new UpstashJSONParseError(rawBody2, { cause: error2 });
      }
      throw new UpstashError(`${body2.error}, command was: ${JSON.stringify(req.body)}`);
    }
    if (this.readYourWrites) {
      const headers = res.headers;
      this.upstashSyncToken = headers.get("upstash-sync-token") ?? "";
    }
    if (isEventStream && req && req.onMessage && res.body) {
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      (async () => {
        try {
          let buffer = "";
          while (true) {
            const { value, done } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            const lines = buffer.split("\n");
            buffer = lines.pop() || "";
            if (buffer.length > MAX_BUFFER_SIZE) {
              throw new Error("Buffer size exceeded (1MB)");
            }
            for (const line of lines) {
              if (line.startsWith("data: ")) {
                const data = line.slice(6);
                req.onMessage?.(data);
              }
            }
          }
        } catch (error2) {
          if (error2 instanceof Error && error2.name === "AbortError") {
          } else {
            console.error("Stream reading error:", error2);
          }
        } finally {
          try {
            await reader.cancel();
          } catch {
          }
        }
      })();
      return { result: 1 };
    }
    let body;
    const rawBody = await res.text();
    try {
      body = JSON.parse(rawBody);
    } catch (error2) {
      throw new UpstashJSONParseError(rawBody, { cause: error2 });
    }
    if (this.readYourWrites) {
      const headers = res.headers;
      this.upstashSyncToken = headers.get("upstash-sync-token") ?? "";
    }
    if (this.options.responseEncoding === "base64") {
      if (Array.isArray(body)) {
        return body.map(({ result: result2, error: error2 }) => ({
          result: decode(result2),
          error: error2
        }));
      }
      const result = decode(body.result);
      return { result, error: body.error };
    }
    return body;
  }
};
function base64decode(b64) {
  let dec = "";
  try {
    const binString = atob(b64);
    const size = binString.length;
    const bytes = new Uint8Array(size);
    for (let i = 0; i < size; i++) {
      bytes[i] = binString.charCodeAt(i);
    }
    dec = new TextDecoder().decode(bytes);
  } catch {
    dec = b64;
  }
  return dec;
}
function decode(raw) {
  let result = void 0;
  switch (typeof raw) {
    case "undefined": {
      return raw;
    }
    case "number": {
      result = raw;
      break;
    }
    case "object": {
      if (Array.isArray(raw)) {
        result = raw.map(
          (v) => typeof v === "string" ? base64decode(v) : Array.isArray(v) ? v.map((element) => decode(element)) : v
        );
      } else {
        result = null;
      }
      break;
    }
    case "string": {
      result = raw === "OK" ? "OK" : base64decode(raw);
      break;
    }
    default: {
      break;
    }
  }
  return result;
}
function merge(obj, key, value) {
  if (!value) {
    return obj;
  }
  obj[key] = obj[key] ? [obj[key], value].join(",") : value;
  return obj;
}
var defaultSerializer = (c) => {
  switch (typeof c) {
    case "string":
    case "number":
    case "boolean": {
      return c;
    }
    default: {
      return JSON.stringify(c);
    }
  }
};
var Command = class {
  command;
  serialize;
  deserialize;
  headers;
  path;
  onMessage;
  isStreaming;
  signal;
  /**
   * Create a new command instance.
   *
   * You can define a custom `deserialize` function. By default we try to deserialize as json.
   */
  constructor(command, opts) {
    this.serialize = defaultSerializer;
    this.deserialize = opts?.automaticDeserialization === void 0 || opts.automaticDeserialization ? opts?.deserialize ?? parseResponse : (x) => x;
    this.command = command.map((c) => this.serialize(c));
    this.headers = opts?.headers;
    this.path = opts?.path;
    this.onMessage = opts?.streamOptions?.onMessage;
    this.isStreaming = opts?.streamOptions?.isStreaming ?? false;
    this.signal = opts?.streamOptions?.signal;
    if (opts?.latencyLogging) {
      const originalExec = this.exec.bind(this);
      this.exec = async (client) => {
        const start = performance.now();
        const result = await originalExec(client);
        const end = performance.now();
        const loggerResult = (end - start).toFixed(2);
        console.log(
          `Latency for \x1B[38;2;19;185;39m${this.command[0].toString().toUpperCase()}\x1B[0m: \x1B[38;2;0;255;255m${loggerResult} ms\x1B[0m`
        );
        return result;
      };
    }
  }
  /**
   * Execute the command using a client.
   */
  async exec(client) {
    const { result, error } = await client.request({
      body: this.command,
      path: this.path,
      upstashSyncToken: client.upstashSyncToken,
      headers: this.headers,
      onMessage: this.onMessage,
      isStreaming: this.isStreaming,
      signal: this.signal
    });
    if (error) {
      throw new UpstashError(error);
    }
    if (result === void 0) {
      throw new TypeError("Request did not return a result");
    }
    return this.deserialize(result);
  }
};
function deserialize(result) {
  if (result.length === 0) {
    return null;
  }
  const obj = {};
  for (let i = 0; i < result.length; i += 2) {
    const key = result[i];
    const value = result[i + 1];
    try {
      obj[key] = JSON.parse(value);
    } catch {
      obj[key] = value;
    }
  }
  return obj;
}
var HRandFieldCommand = class extends Command {
  constructor(cmd, opts) {
    const command = ["hrandfield", cmd[0]];
    if (typeof cmd[1] === "number") {
      command.push(cmd[1]);
    }
    if (cmd[2]) {
      command.push("WITHVALUES");
    }
    super(command, {
      // @ts-expect-error to silence compiler
      deserialize: cmd[2] ? (result) => deserialize(result) : opts?.deserialize,
      ...opts
    });
  }
};
var AppendCommand = class extends Command {
  constructor(cmd, opts) {
    super(["append", ...cmd], opts);
  }
};
var BitCountCommand = class extends Command {
  constructor([key, start, end], opts) {
    const command = ["bitcount", key];
    if (typeof start === "number") {
      command.push(start);
    }
    if (typeof end === "number") {
      command.push(end);
    }
    super(command, opts);
  }
};
var BitFieldCommand = class {
  constructor(args, client, opts, execOperation = (command) => command.exec(this.client)) {
    this.client = client;
    this.opts = opts;
    this.execOperation = execOperation;
    this.command = ["bitfield", ...args];
  }
  command;
  chain(...args) {
    this.command.push(...args);
    return this;
  }
  get(...args) {
    return this.chain("get", ...args);
  }
  set(...args) {
    return this.chain("set", ...args);
  }
  incrby(...args) {
    return this.chain("incrby", ...args);
  }
  overflow(overflow) {
    return this.chain("overflow", overflow);
  }
  exec() {
    const command = new Command(this.command, this.opts);
    return this.execOperation(command);
  }
};
var BitOpCommand = class extends Command {
  constructor(cmd, opts) {
    super(["bitop", ...cmd], opts);
  }
};
var BitPosCommand = class extends Command {
  constructor(cmd, opts) {
    super(["bitpos", ...cmd], opts);
  }
};
var CopyCommand = class extends Command {
  constructor([key, destinationKey, opts], commandOptions) {
    super(["COPY", key, destinationKey, ...opts?.replace ? ["REPLACE"] : []], {
      ...commandOptions,
      deserialize(result) {
        if (result > 0) {
          return "COPIED";
        }
        return "NOT_COPIED";
      }
    });
  }
};
var DBSizeCommand = class extends Command {
  constructor(opts) {
    super(["dbsize"], opts);
  }
};
var DecrCommand = class extends Command {
  constructor(cmd, opts) {
    super(["decr", ...cmd], opts);
  }
};
var DecrByCommand = class extends Command {
  constructor(cmd, opts) {
    super(["decrby", ...cmd], opts);
  }
};
var DelCommand = class extends Command {
  constructor(cmd, opts) {
    super(["del", ...cmd], opts);
  }
};
var EchoCommand = class extends Command {
  constructor(cmd, opts) {
    super(["echo", ...cmd], opts);
  }
};
var EvalROCommand = class extends Command {
  constructor([script, keys, args], opts) {
    super(["eval_ro", script, keys.length, ...keys, ...args ?? []], opts);
  }
};
var EvalCommand = class extends Command {
  constructor([script, keys, args], opts) {
    super(["eval", script, keys.length, ...keys, ...args ?? []], opts);
  }
};
var EvalshaROCommand = class extends Command {
  constructor([sha, keys, args], opts) {
    super(["evalsha_ro", sha, keys.length, ...keys, ...args ?? []], opts);
  }
};
var EvalshaCommand = class extends Command {
  constructor([sha, keys, args], opts) {
    super(["evalsha", sha, keys.length, ...keys, ...args ?? []], opts);
  }
};
var ExecCommand = class extends Command {
  constructor(cmd, opts) {
    const normalizedCmd = cmd.map((arg) => typeof arg === "string" ? arg : String(arg));
    super(normalizedCmd, opts);
  }
};
var ExistsCommand = class extends Command {
  constructor(cmd, opts) {
    super(["exists", ...cmd], opts);
  }
};
var ExpireCommand = class extends Command {
  constructor(cmd, opts) {
    super(["expire", ...cmd.filter(Boolean)], opts);
  }
};
var ExpireAtCommand = class extends Command {
  constructor(cmd, opts) {
    super(["expireat", ...cmd], opts);
  }
};
var FCallCommand = class extends Command {
  constructor([functionName, keys, args], opts) {
    super(["fcall", functionName, ...keys ? [keys.length, ...keys] : [0], ...args ?? []], opts);
  }
};
var FCallRoCommand = class extends Command {
  constructor([functionName, keys, args], opts) {
    super(
      ["fcall_ro", functionName, ...keys ? [keys.length, ...keys] : [0], ...args ?? []],
      opts
    );
  }
};
var FlushAllCommand = class extends Command {
  constructor(args, opts) {
    const command = ["flushall"];
    if (args && args.length > 0 && args[0].async) {
      command.push("async");
    }
    super(command, opts);
  }
};
var FlushDBCommand = class extends Command {
  constructor([opts], cmdOpts) {
    const command = ["flushdb"];
    if (opts?.async) {
      command.push("async");
    }
    super(command, cmdOpts);
  }
};
var FunctionDeleteCommand = class extends Command {
  constructor([libraryName], opts) {
    super(["function", "delete", libraryName], opts);
  }
};
var FunctionFlushCommand = class extends Command {
  constructor(opts) {
    super(["function", "flush"], opts);
  }
};
var FunctionListCommand = class extends Command {
  constructor([args], opts) {
    const command = ["function", "list"];
    if (args?.libraryName) {
      command.push("libraryname", args.libraryName);
    }
    if (args?.withCode) {
      command.push("withcode");
    }
    super(command, { deserialize: deserialize2, ...opts });
  }
};
function deserialize2(result) {
  if (!Array.isArray(result)) return [];
  return result.map((libRaw) => {
    const lib = kvArrayToObject(libRaw);
    const functionsParsed = lib.functions.map(
      (fnRaw) => kvArrayToObject(fnRaw)
    );
    return {
      libraryName: lib.library_name,
      engine: lib.engine,
      functions: functionsParsed.map((fn) => ({
        name: fn.name,
        description: fn.description ?? void 0,
        flags: fn.flags
      })),
      libraryCode: lib.library_code
    };
  });
}
var FunctionLoadCommand = class extends Command {
  constructor([args], opts) {
    super(["function", "load", ...args.replace ? ["replace"] : [], args.code], opts);
  }
};
var FunctionStatsCommand = class extends Command {
  constructor(opts) {
    super(["function", "stats"], { deserialize: deserialize3, ...opts });
  }
};
function deserialize3(result) {
  const rawEngines = kvArrayToObject(kvArrayToObject(result).engines);
  const parsedEngines = Object.fromEntries(
    Object.entries(rawEngines).map(([key, value]) => [key, kvArrayToObject(value)])
  );
  const final = {
    engines: Object.fromEntries(
      Object.entries(parsedEngines).map(([key, value]) => [
        key,
        {
          librariesCount: value.libraries_count,
          functionsCount: value.functions_count
        }
      ])
    )
  };
  return final;
}
var GeoAddCommand = class extends Command {
  constructor([key, arg1, ...arg2], opts) {
    const command = ["geoadd", key];
    if ("nx" in arg1 && arg1.nx) {
      command.push("nx");
    } else if ("xx" in arg1 && arg1.xx) {
      command.push("xx");
    }
    if ("ch" in arg1 && arg1.ch) {
      command.push("ch");
    }
    if ("latitude" in arg1 && arg1.latitude) {
      command.push(arg1.longitude, arg1.latitude, arg1.member);
    }
    command.push(
      ...arg2.flatMap(({ latitude, longitude, member }) => [longitude, latitude, member])
    );
    super(command, opts);
  }
};
var GeoDistCommand = class extends Command {
  constructor([key, member1, member2, unit = "M"], opts) {
    super(["GEODIST", key, member1, member2, unit], opts);
  }
};
var GeoHashCommand = class extends Command {
  constructor(cmd, opts) {
    const [key] = cmd;
    const members = Array.isArray(cmd[1]) ? cmd[1] : cmd.slice(1);
    super(["GEOHASH", key, ...members], opts);
  }
};
var GeoPosCommand = class extends Command {
  constructor(cmd, opts) {
    const [key] = cmd;
    const members = Array.isArray(cmd[1]) ? cmd[1] : cmd.slice(1);
    super(["GEOPOS", key, ...members], {
      deserialize: (result) => transform(result),
      ...opts
    });
  }
};
function transform(result) {
  const final = [];
  for (const pos of result) {
    if (!pos?.[0] || !pos?.[1]) {
      continue;
    }
    final.push({ lng: Number.parseFloat(pos[0]), lat: Number.parseFloat(pos[1]) });
  }
  return final;
}
var GeoSearchCommand = class extends Command {
  constructor([key, centerPoint, shape, order, opts], commandOptions) {
    const command = ["GEOSEARCH", key];
    if (centerPoint.type === "FROMMEMBER" || centerPoint.type === "frommember") {
      command.push(centerPoint.type, centerPoint.member);
    }
    if (centerPoint.type === "FROMLONLAT" || centerPoint.type === "fromlonlat") {
      command.push(centerPoint.type, centerPoint.coordinate.lon, centerPoint.coordinate.lat);
    }
    if (shape.type === "BYRADIUS" || shape.type === "byradius") {
      command.push(shape.type, shape.radius, shape.radiusType);
    }
    if (shape.type === "BYBOX" || shape.type === "bybox") {
      command.push(shape.type, shape.rect.width, shape.rect.height, shape.rectType);
    }
    command.push(order);
    if (opts?.count) {
      command.push("COUNT", opts.count.limit, ...opts.count.any ? ["ANY"] : []);
    }
    const transform2 = (result) => {
      if (!opts?.withCoord && !opts?.withDist && !opts?.withHash) {
        return result.map((member) => {
          try {
            return { member: JSON.parse(member) };
          } catch {
            return { member };
          }
        });
      }
      return result.map((members) => {
        let counter = 1;
        const obj = {};
        try {
          obj.member = JSON.parse(members[0]);
        } catch {
          obj.member = members[0];
        }
        if (opts.withDist) {
          obj.dist = Number.parseFloat(members[counter++]);
        }
        if (opts.withHash) {
          obj.hash = members[counter++].toString();
        }
        if (opts.withCoord) {
          obj.coord = {
            long: Number.parseFloat(members[counter][0]),
            lat: Number.parseFloat(members[counter][1])
          };
        }
        return obj;
      });
    };
    super(
      [
        ...command,
        ...opts?.withCoord ? ["WITHCOORD"] : [],
        ...opts?.withDist ? ["WITHDIST"] : [],
        ...opts?.withHash ? ["WITHHASH"] : []
      ],
      {
        deserialize: transform2,
        ...commandOptions
      }
    );
  }
};
var GeoSearchStoreCommand = class extends Command {
  constructor([destination, key, centerPoint, shape, order, opts], commandOptions) {
    const command = ["GEOSEARCHSTORE", destination, key];
    if (centerPoint.type === "FROMMEMBER" || centerPoint.type === "frommember") {
      command.push(centerPoint.type, centerPoint.member);
    }
    if (centerPoint.type === "FROMLONLAT" || centerPoint.type === "fromlonlat") {
      command.push(centerPoint.type, centerPoint.coordinate.lon, centerPoint.coordinate.lat);
    }
    if (shape.type === "BYRADIUS" || shape.type === "byradius") {
      command.push(shape.type, shape.radius, shape.radiusType);
    }
    if (shape.type === "BYBOX" || shape.type === "bybox") {
      command.push(shape.type, shape.rect.width, shape.rect.height, shape.rectType);
    }
    command.push(order);
    if (opts?.count) {
      command.push("COUNT", opts.count.limit, ...opts.count.any ? ["ANY"] : []);
    }
    super([...command, ...opts?.storeDist ? ["STOREDIST"] : []], commandOptions);
  }
};
var GetCommand = class extends Command {
  constructor(cmd, opts) {
    super(["get", ...cmd], opts);
  }
};
var GetBitCommand = class extends Command {
  constructor(cmd, opts) {
    super(["getbit", ...cmd], opts);
  }
};
var GetDelCommand = class extends Command {
  constructor(cmd, opts) {
    super(["getdel", ...cmd], opts);
  }
};
var GetExCommand = class extends Command {
  constructor([key, opts], cmdOpts) {
    const command = ["getex", key];
    if (opts) {
      if ("ex" in opts && typeof opts.ex === "number") {
        command.push("ex", opts.ex);
      } else if ("px" in opts && typeof opts.px === "number") {
        command.push("px", opts.px);
      } else if ("exat" in opts && typeof opts.exat === "number") {
        command.push("exat", opts.exat);
      } else if ("pxat" in opts && typeof opts.pxat === "number") {
        command.push("pxat", opts.pxat);
      } else if ("persist" in opts && opts.persist) {
        command.push("persist");
      }
    }
    super(command, cmdOpts);
  }
};
var GetRangeCommand = class extends Command {
  constructor(cmd, opts) {
    super(["getrange", ...cmd], opts);
  }
};
var GetSetCommand = class extends Command {
  constructor(cmd, opts) {
    super(["getset", ...cmd], opts);
  }
};
var HDelCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hdel", ...cmd], opts);
  }
};
var HExistsCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hexists", ...cmd], opts);
  }
};
var HExpireCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, fields, seconds, option] = cmd;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    super(
      [
        "hexpire",
        key,
        seconds,
        ...option ? [option] : [],
        "FIELDS",
        fieldArray.length,
        ...fieldArray
      ],
      opts
    );
  }
};
var HExpireAtCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, fields, timestamp, option] = cmd;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    super(
      [
        "hexpireat",
        key,
        timestamp,
        ...option ? [option] : [],
        "FIELDS",
        fieldArray.length,
        ...fieldArray
      ],
      opts
    );
  }
};
var HExpireTimeCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, fields] = cmd;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    super(["hexpiretime", key, "FIELDS", fieldArray.length, ...fieldArray], opts);
  }
};
var HPersistCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, fields] = cmd;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    super(["hpersist", key, "FIELDS", fieldArray.length, ...fieldArray], opts);
  }
};
var HPExpireCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, fields, milliseconds, option] = cmd;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    super(
      [
        "hpexpire",
        key,
        milliseconds,
        ...option ? [option] : [],
        "FIELDS",
        fieldArray.length,
        ...fieldArray
      ],
      opts
    );
  }
};
var HPExpireAtCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, fields, timestamp, option] = cmd;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    super(
      [
        "hpexpireat",
        key,
        timestamp,
        ...option ? [option] : [],
        "FIELDS",
        fieldArray.length,
        ...fieldArray
      ],
      opts
    );
  }
};
var HPExpireTimeCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, fields] = cmd;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    super(["hpexpiretime", key, "FIELDS", fieldArray.length, ...fieldArray], opts);
  }
};
var HPTtlCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, fields] = cmd;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    super(["hpttl", key, "FIELDS", fieldArray.length, ...fieldArray], opts);
  }
};
var HGetCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hget", ...cmd], opts);
  }
};
function deserialize4(result) {
  if (result.length === 0) {
    return null;
  }
  const obj = {};
  for (let i = 0; i < result.length; i += 2) {
    const key = result[i];
    const value = result[i + 1];
    try {
      const valueIsNumberAndNotSafeInteger = !Number.isNaN(Number(value)) && !Number.isSafeInteger(Number(value));
      obj[key] = valueIsNumberAndNotSafeInteger ? value : JSON.parse(value);
    } catch {
      obj[key] = value;
    }
  }
  return obj;
}
var HGetAllCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hgetall", ...cmd], {
      deserialize: (result) => deserialize4(result),
      ...opts
    });
  }
};
var HIncrByCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hincrby", ...cmd], opts);
  }
};
var HIncrByFloatCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hincrbyfloat", ...cmd], opts);
  }
};
var HKeysCommand = class extends Command {
  constructor([key], opts) {
    super(["hkeys", key], opts);
  }
};
var HLenCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hlen", ...cmd], opts);
  }
};
function deserialize5(fields, result) {
  if (result.every((field) => field === null)) {
    return null;
  }
  const obj = {};
  for (const [i, field] of fields.entries()) {
    try {
      obj[field] = JSON.parse(result[i]);
    } catch {
      obj[field] = result[i];
    }
  }
  return obj;
}
var HMGetCommand = class extends Command {
  constructor([key, ...fields], opts) {
    super(["hmget", key, ...fields], {
      deserialize: (result) => deserialize5(fields, result),
      ...opts
    });
  }
};
var HMSetCommand = class extends Command {
  constructor([key, kv], opts) {
    super(["hmset", key, ...Object.entries(kv).flatMap(([field, value]) => [field, value])], opts);
  }
};
var HScanCommand = class extends Command {
  constructor([key, cursor, cmdOpts], opts) {
    const command = ["hscan", key, cursor];
    if (cmdOpts?.match) {
      command.push("match", cmdOpts.match);
    }
    if (typeof cmdOpts?.count === "number") {
      command.push("count", cmdOpts.count);
    }
    super(command, {
      deserialize: deserializeScanResponse,
      ...opts
    });
  }
};
var HSetCommand = class extends Command {
  constructor([key, kv], opts) {
    super(["hset", key, ...Object.entries(kv).flatMap(([field, value]) => [field, value])], opts);
  }
};
var HSetNXCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hsetnx", ...cmd], opts);
  }
};
var HStrLenCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hstrlen", ...cmd], opts);
  }
};
var HTtlCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, fields] = cmd;
    const fieldArray = Array.isArray(fields) ? fields : [fields];
    super(["httl", key, "FIELDS", fieldArray.length, ...fieldArray], opts);
  }
};
var HValsCommand = class extends Command {
  constructor(cmd, opts) {
    super(["hvals", ...cmd], opts);
  }
};
var IncrCommand = class extends Command {
  constructor(cmd, opts) {
    super(["incr", ...cmd], opts);
  }
};
var IncrByCommand = class extends Command {
  constructor(cmd, opts) {
    super(["incrby", ...cmd], opts);
  }
};
var IncrByFloatCommand = class extends Command {
  constructor(cmd, opts) {
    super(["incrbyfloat", ...cmd], opts);
  }
};
var JsonArrAppendCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.ARRAPPEND", ...cmd], opts);
  }
};
var JsonArrIndexCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.ARRINDEX", ...cmd], opts);
  }
};
var JsonArrInsertCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.ARRINSERT", ...cmd], opts);
  }
};
var JsonArrLenCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.ARRLEN", cmd[0], cmd[1] ?? "$"], opts);
  }
};
var JsonArrPopCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.ARRPOP", ...cmd], opts);
  }
};
var JsonArrTrimCommand = class extends Command {
  constructor(cmd, opts) {
    const path = cmd[1] ?? "$";
    const start = cmd[2] ?? 0;
    const stop = cmd[3] ?? 0;
    super(["JSON.ARRTRIM", cmd[0], path, start, stop], opts);
  }
};
var JsonClearCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.CLEAR", ...cmd], opts);
  }
};
var JsonDelCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.DEL", ...cmd], opts);
  }
};
var JsonForgetCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.FORGET", ...cmd], opts);
  }
};
var JsonGetCommand = class extends Command {
  constructor(cmd, opts) {
    const command = ["JSON.GET"];
    if (typeof cmd[1] === "string") {
      command.push(...cmd);
    } else {
      command.push(cmd[0]);
      if (cmd[1]) {
        if (cmd[1].indent) {
          command.push("INDENT", cmd[1].indent);
        }
        if (cmd[1].newline) {
          command.push("NEWLINE", cmd[1].newline);
        }
        if (cmd[1].space) {
          command.push("SPACE", cmd[1].space);
        }
      }
      command.push(...cmd.slice(2));
    }
    super(command, opts);
  }
};
var JsonMergeCommand = class extends Command {
  constructor(cmd, opts) {
    const command = ["JSON.MERGE", ...cmd];
    super(command, opts);
  }
};
var JsonMGetCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.MGET", ...cmd[0], cmd[1]], opts);
  }
};
var JsonMSetCommand = class extends Command {
  constructor(cmd, opts) {
    const command = ["JSON.MSET"];
    for (const c of cmd) {
      command.push(c.key, c.path, c.value);
    }
    super(command, opts);
  }
};
var JsonNumIncrByCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.NUMINCRBY", ...cmd], opts);
  }
};
var JsonNumMultByCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.NUMMULTBY", ...cmd], opts);
  }
};
var JsonObjKeysCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.OBJKEYS", ...cmd], opts);
  }
};
var JsonObjLenCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.OBJLEN", ...cmd], opts);
  }
};
var JsonRespCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.RESP", ...cmd], opts);
  }
};
var JsonSetCommand = class extends Command {
  constructor(cmd, opts) {
    const command = ["JSON.SET", cmd[0], cmd[1], cmd[2]];
    if (cmd[3]) {
      if (cmd[3].nx) {
        command.push("NX");
      } else if (cmd[3].xx) {
        command.push("XX");
      }
    }
    super(command, opts);
  }
};
var JsonStrAppendCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.STRAPPEND", ...cmd], opts);
  }
};
var JsonStrLenCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.STRLEN", ...cmd], opts);
  }
};
var JsonToggleCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.TOGGLE", ...cmd], opts);
  }
};
var JsonTypeCommand = class extends Command {
  constructor(cmd, opts) {
    super(["JSON.TYPE", ...cmd], opts);
  }
};
var KeysCommand = class extends Command {
  constructor(cmd, opts) {
    super(["keys", ...cmd], opts);
  }
};
var LIndexCommand = class extends Command {
  constructor(cmd, opts) {
    super(["lindex", ...cmd], opts);
  }
};
var LInsertCommand = class extends Command {
  constructor(cmd, opts) {
    super(["linsert", ...cmd], opts);
  }
};
var LLenCommand = class extends Command {
  constructor(cmd, opts) {
    super(["llen", ...cmd], opts);
  }
};
var LMoveCommand = class extends Command {
  constructor(cmd, opts) {
    super(["lmove", ...cmd], opts);
  }
};
var LmPopCommand = class extends Command {
  constructor(cmd, opts) {
    const [numkeys, keys, direction, count] = cmd;
    super(["LMPOP", numkeys, ...keys, direction, ...count ? ["COUNT", count] : []], opts);
  }
};
var LPopCommand = class extends Command {
  constructor(cmd, opts) {
    super(["lpop", ...cmd], opts);
  }
};
var LPosCommand = class extends Command {
  constructor(cmd, opts) {
    const args = ["lpos", cmd[0], cmd[1]];
    if (typeof cmd[2]?.rank === "number") {
      args.push("rank", cmd[2].rank);
    }
    if (typeof cmd[2]?.count === "number") {
      args.push("count", cmd[2].count);
    }
    if (typeof cmd[2]?.maxLen === "number") {
      args.push("maxLen", cmd[2].maxLen);
    }
    super(args, opts);
  }
};
var LPushCommand = class extends Command {
  constructor(cmd, opts) {
    super(["lpush", ...cmd], opts);
  }
};
var LPushXCommand = class extends Command {
  constructor(cmd, opts) {
    super(["lpushx", ...cmd], opts);
  }
};
var LRangeCommand = class extends Command {
  constructor(cmd, opts) {
    super(["lrange", ...cmd], opts);
  }
};
var LRemCommand = class extends Command {
  constructor(cmd, opts) {
    super(["lrem", ...cmd], opts);
  }
};
var LSetCommand = class extends Command {
  constructor(cmd, opts) {
    super(["lset", ...cmd], opts);
  }
};
var LTrimCommand = class extends Command {
  constructor(cmd, opts) {
    super(["ltrim", ...cmd], opts);
  }
};
var MGetCommand = class extends Command {
  constructor(cmd, opts) {
    const keys = Array.isArray(cmd[0]) ? cmd[0] : cmd;
    super(["mget", ...keys], opts);
  }
};
var MSetCommand = class extends Command {
  constructor([kv], opts) {
    super(["mset", ...Object.entries(kv).flatMap(([key, value]) => [key, value])], opts);
  }
};
var MSetNXCommand = class extends Command {
  constructor([kv], opts) {
    super(["msetnx", ...Object.entries(kv).flat()], opts);
  }
};
var PersistCommand = class extends Command {
  constructor(cmd, opts) {
    super(["persist", ...cmd], opts);
  }
};
var PExpireCommand = class extends Command {
  constructor(cmd, opts) {
    super(["pexpire", ...cmd], opts);
  }
};
var PExpireAtCommand = class extends Command {
  constructor(cmd, opts) {
    super(["pexpireat", ...cmd], opts);
  }
};
var PfAddCommand = class extends Command {
  constructor(cmd, opts) {
    super(["pfadd", ...cmd], opts);
  }
};
var PfCountCommand = class extends Command {
  constructor(cmd, opts) {
    super(["pfcount", ...cmd], opts);
  }
};
var PfMergeCommand = class extends Command {
  constructor(cmd, opts) {
    super(["pfmerge", ...cmd], opts);
  }
};
var PingCommand = class extends Command {
  constructor(cmd, opts) {
    const command = ["ping"];
    if (cmd?.[0] !== void 0) {
      command.push(cmd[0]);
    }
    super(command, opts);
  }
};
var PSetEXCommand = class extends Command {
  constructor(cmd, opts) {
    super(["psetex", ...cmd], opts);
  }
};
var PTtlCommand = class extends Command {
  constructor(cmd, opts) {
    super(["pttl", ...cmd], opts);
  }
};
var PublishCommand = class extends Command {
  constructor(cmd, opts) {
    super(["publish", ...cmd], opts);
  }
};
var RandomKeyCommand = class extends Command {
  constructor(opts) {
    super(["randomkey"], opts);
  }
};
var RenameCommand = class extends Command {
  constructor(cmd, opts) {
    super(["rename", ...cmd], opts);
  }
};
var RenameNXCommand = class extends Command {
  constructor(cmd, opts) {
    super(["renamenx", ...cmd], opts);
  }
};
var RPopCommand = class extends Command {
  constructor(cmd, opts) {
    super(["rpop", ...cmd], opts);
  }
};
var RPushCommand = class extends Command {
  constructor(cmd, opts) {
    super(["rpush", ...cmd], opts);
  }
};
var RPushXCommand = class extends Command {
  constructor(cmd, opts) {
    super(["rpushx", ...cmd], opts);
  }
};
var SAddCommand = class extends Command {
  constructor(cmd, opts) {
    super(["sadd", ...cmd], opts);
  }
};
var ScanCommand = class extends Command {
  constructor([cursor, opts], cmdOpts) {
    const command = ["scan", cursor];
    if (opts?.match) {
      command.push("match", opts.match);
    }
    if (typeof opts?.count === "number") {
      command.push("count", opts.count);
    }
    if (opts && "withType" in opts && opts.withType === true) {
      command.push("withtype");
    } else if (opts && "type" in opts && opts.type && opts.type.length > 0) {
      command.push("type", opts.type);
    }
    super(command, {
      // @ts-expect-error ignore types here
      deserialize: opts?.withType ? deserializeScanWithTypesResponse : deserializeScanResponse,
      ...cmdOpts
    });
  }
};
var SCardCommand = class extends Command {
  constructor(cmd, opts) {
    super(["scard", ...cmd], opts);
  }
};
var ScriptExistsCommand = class extends Command {
  constructor(hashes, opts) {
    super(["script", "exists", ...hashes], {
      deserialize: (result) => result,
      ...opts
    });
  }
};
var ScriptFlushCommand = class extends Command {
  constructor([opts], cmdOpts) {
    const cmd = ["script", "flush"];
    if (opts?.sync) {
      cmd.push("sync");
    } else if (opts?.async) {
      cmd.push("async");
    }
    super(cmd, cmdOpts);
  }
};
var ScriptLoadCommand = class extends Command {
  constructor(args, opts) {
    super(["script", "load", ...args], opts);
  }
};
var SDiffCommand = class extends Command {
  constructor(cmd, opts) {
    super(["sdiff", ...cmd], opts);
  }
};
var SDiffStoreCommand = class extends Command {
  constructor(cmd, opts) {
    super(["sdiffstore", ...cmd], opts);
  }
};
var SetCommand = class extends Command {
  constructor([key, value, opts], cmdOpts) {
    const command = ["set", key, value];
    if (opts) {
      if ("nx" in opts && opts.nx) {
        command.push("nx");
      } else if ("xx" in opts && opts.xx) {
        command.push("xx");
      }
      if ("get" in opts && opts.get) {
        command.push("get");
      }
      if ("ex" in opts && typeof opts.ex === "number") {
        command.push("ex", opts.ex);
      } else if ("px" in opts && typeof opts.px === "number") {
        command.push("px", opts.px);
      } else if ("exat" in opts && typeof opts.exat === "number") {
        command.push("exat", opts.exat);
      } else if ("pxat" in opts && typeof opts.pxat === "number") {
        command.push("pxat", opts.pxat);
      } else if ("keepTtl" in opts && opts.keepTtl) {
        command.push("keepTtl");
      }
    }
    super(command, cmdOpts);
  }
};
var SetBitCommand = class extends Command {
  constructor(cmd, opts) {
    super(["setbit", ...cmd], opts);
  }
};
var SetExCommand = class extends Command {
  constructor(cmd, opts) {
    super(["setex", ...cmd], opts);
  }
};
var SetNxCommand = class extends Command {
  constructor(cmd, opts) {
    super(["setnx", ...cmd], opts);
  }
};
var SetRangeCommand = class extends Command {
  constructor(cmd, opts) {
    super(["setrange", ...cmd], opts);
  }
};
var SInterCommand = class extends Command {
  constructor(cmd, opts) {
    super(["sinter", ...cmd], opts);
  }
};
var SInterStoreCommand = class extends Command {
  constructor(cmd, opts) {
    super(["sinterstore", ...cmd], opts);
  }
};
var SIsMemberCommand = class extends Command {
  constructor(cmd, opts) {
    super(["sismember", ...cmd], opts);
  }
};
var SMembersCommand = class extends Command {
  constructor(cmd, opts) {
    super(["smembers", ...cmd], opts);
  }
};
var SMIsMemberCommand = class extends Command {
  constructor(cmd, opts) {
    super(["smismember", cmd[0], ...cmd[1]], opts);
  }
};
var SMoveCommand = class extends Command {
  constructor(cmd, opts) {
    super(["smove", ...cmd], opts);
  }
};
var SPopCommand = class extends Command {
  constructor([key, count], opts) {
    const command = ["spop", key];
    if (typeof count === "number") {
      command.push(count);
    }
    super(command, opts);
  }
};
var SRandMemberCommand = class extends Command {
  constructor([key, count], opts) {
    const command = ["srandmember", key];
    if (typeof count === "number") {
      command.push(count);
    }
    super(command, opts);
  }
};
var SRemCommand = class extends Command {
  constructor(cmd, opts) {
    super(["srem", ...cmd], opts);
  }
};
var SScanCommand = class extends Command {
  constructor([key, cursor, opts], cmdOpts) {
    const command = ["sscan", key, cursor];
    if (opts?.match) {
      command.push("match", opts.match);
    }
    if (typeof opts?.count === "number") {
      command.push("count", opts.count);
    }
    super(command, {
      deserialize: deserializeScanResponse,
      ...cmdOpts
    });
  }
};
var StrLenCommand = class extends Command {
  constructor(cmd, opts) {
    super(["strlen", ...cmd], opts);
  }
};
var SUnionCommand = class extends Command {
  constructor(cmd, opts) {
    super(["sunion", ...cmd], opts);
  }
};
var SUnionStoreCommand = class extends Command {
  constructor(cmd, opts) {
    super(["sunionstore", ...cmd], opts);
  }
};
var TimeCommand = class extends Command {
  constructor(opts) {
    super(["time"], opts);
  }
};
var TouchCommand = class extends Command {
  constructor(cmd, opts) {
    super(["touch", ...cmd], opts);
  }
};
var TtlCommand = class extends Command {
  constructor(cmd, opts) {
    super(["ttl", ...cmd], opts);
  }
};
var TypeCommand = class extends Command {
  constructor(cmd, opts) {
    super(["type", ...cmd], opts);
  }
};
var UnlinkCommand = class extends Command {
  constructor(cmd, opts) {
    super(["unlink", ...cmd], opts);
  }
};
var XAckCommand = class extends Command {
  constructor([key, group, id], opts) {
    const ids = Array.isArray(id) ? [...id] : [id];
    super(["XACK", key, group, ...ids], opts);
  }
};
var XAddCommand = class extends Command {
  constructor([key, id, entries, opts], commandOptions) {
    const command = ["XADD", key];
    if (opts) {
      if (opts.nomkStream) {
        command.push("NOMKSTREAM");
      }
      if (opts.trim) {
        command.push(opts.trim.type, opts.trim.comparison, opts.trim.threshold);
        if (opts.trim.limit !== void 0) {
          command.push("LIMIT", opts.trim.limit);
        }
      }
    }
    command.push(id);
    for (const [k, v] of Object.entries(entries)) {
      command.push(k, v);
    }
    super(command, commandOptions);
  }
};
var XAutoClaim = class extends Command {
  constructor([key, group, consumer, minIdleTime, start, options], opts) {
    const commands = [];
    if (options?.count) {
      commands.push("COUNT", options.count);
    }
    if (options?.justId) {
      commands.push("JUSTID");
    }
    super(["XAUTOCLAIM", key, group, consumer, minIdleTime, start, ...commands], opts);
  }
};
var XClaimCommand = class extends Command {
  constructor([key, group, consumer, minIdleTime, id, options], opts) {
    const ids = Array.isArray(id) ? [...id] : [id];
    const commands = [];
    if (options?.idleMS) {
      commands.push("IDLE", options.idleMS);
    }
    if (options?.idleMS) {
      commands.push("TIME", options.timeMS);
    }
    if (options?.retryCount) {
      commands.push("RETRYCOUNT", options.retryCount);
    }
    if (options?.force) {
      commands.push("FORCE");
    }
    if (options?.justId) {
      commands.push("JUSTID");
    }
    if (options?.lastId) {
      commands.push("LASTID", options.lastId);
    }
    super(["XCLAIM", key, group, consumer, minIdleTime, ...ids, ...commands], opts);
  }
};
var XDelCommand = class extends Command {
  constructor([key, ids], opts) {
    const cmds = Array.isArray(ids) ? [...ids] : [ids];
    super(["XDEL", key, ...cmds], opts);
  }
};
var XGroupCommand = class extends Command {
  constructor([key, opts], commandOptions) {
    const command = ["XGROUP"];
    switch (opts.type) {
      case "CREATE": {
        command.push("CREATE", key, opts.group, opts.id);
        if (opts.options) {
          if (opts.options.MKSTREAM) {
            command.push("MKSTREAM");
          }
          if (opts.options.ENTRIESREAD !== void 0) {
            command.push("ENTRIESREAD", opts.options.ENTRIESREAD.toString());
          }
        }
        break;
      }
      case "CREATECONSUMER": {
        command.push("CREATECONSUMER", key, opts.group, opts.consumer);
        break;
      }
      case "DELCONSUMER": {
        command.push("DELCONSUMER", key, opts.group, opts.consumer);
        break;
      }
      case "DESTROY": {
        command.push("DESTROY", key, opts.group);
        break;
      }
      case "SETID": {
        command.push("SETID", key, opts.group, opts.id);
        if (opts.options?.ENTRIESREAD !== void 0) {
          command.push("ENTRIESREAD", opts.options.ENTRIESREAD.toString());
        }
        break;
      }
      default: {
        throw new Error("Invalid XGROUP");
      }
    }
    super(command, commandOptions);
  }
};
var XInfoCommand = class extends Command {
  constructor([key, options], opts) {
    const cmds = [];
    if (options.type === "CONSUMERS") {
      cmds.push("CONSUMERS", key, options.group);
    } else {
      cmds.push("GROUPS", key);
    }
    super(["XINFO", ...cmds], opts);
  }
};
var XLenCommand = class extends Command {
  constructor(cmd, opts) {
    super(["XLEN", ...cmd], opts);
  }
};
var XPendingCommand = class extends Command {
  constructor([key, group, start, end, count, options], opts) {
    const consumers = options?.consumer === void 0 ? [] : Array.isArray(options.consumer) ? [...options.consumer] : [options.consumer];
    super(
      [
        "XPENDING",
        key,
        group,
        ...options?.idleTime ? ["IDLE", options.idleTime] : [],
        start,
        end,
        count,
        ...consumers
      ],
      opts
    );
  }
};
function deserialize6(result) {
  const obj = {};
  for (const e of result) {
    for (let i = 0; i < e.length; i += 2) {
      const streamId = e[i];
      const entries = e[i + 1];
      if (!(streamId in obj)) {
        obj[streamId] = {};
      }
      for (let j = 0; j < entries.length; j += 2) {
        const field = entries[j];
        const value = entries[j + 1];
        try {
          obj[streamId][field] = JSON.parse(value);
        } catch {
          obj[streamId][field] = value;
        }
      }
    }
  }
  return obj;
}
var XRangeCommand = class extends Command {
  constructor([key, start, end, count], opts) {
    const command = ["XRANGE", key, start, end];
    if (typeof count === "number") {
      command.push("COUNT", count);
    }
    super(command, {
      deserialize: (result) => deserialize6(result),
      ...opts
    });
  }
};
var UNBALANCED_XREAD_ERR = "ERR Unbalanced XREAD list of streams: for each stream key an ID or '$' must be specified";
var XReadCommand = class extends Command {
  constructor([key, id, options], opts) {
    if (Array.isArray(key) && Array.isArray(id) && key.length !== id.length) {
      throw new Error(UNBALANCED_XREAD_ERR);
    }
    const commands = [];
    if (typeof options?.count === "number") {
      commands.push("COUNT", options.count);
    }
    if (typeof options?.blockMS === "number") {
      commands.push("BLOCK", options.blockMS);
    }
    commands.push(
      "STREAMS",
      ...Array.isArray(key) ? [...key] : [key],
      ...Array.isArray(id) ? [...id] : [id]
    );
    super(["XREAD", ...commands], opts);
  }
};
var UNBALANCED_XREADGROUP_ERR = "ERR Unbalanced XREADGROUP list of streams: for each stream key an ID or '$' must be specified";
var XReadGroupCommand = class extends Command {
  constructor([group, consumer, key, id, options], opts) {
    if (Array.isArray(key) && Array.isArray(id) && key.length !== id.length) {
      throw new Error(UNBALANCED_XREADGROUP_ERR);
    }
    const commands = [];
    if (typeof options?.count === "number") {
      commands.push("COUNT", options.count);
    }
    if (typeof options?.blockMS === "number") {
      commands.push("BLOCK", options.blockMS);
    }
    if (typeof options?.NOACK === "boolean" && options.NOACK) {
      commands.push("NOACK");
    }
    commands.push(
      "STREAMS",
      ...Array.isArray(key) ? [...key] : [key],
      ...Array.isArray(id) ? [...id] : [id]
    );
    super(["XREADGROUP", "GROUP", group, consumer, ...commands], opts);
  }
};
var XRevRangeCommand = class extends Command {
  constructor([key, end, start, count], opts) {
    const command = ["XREVRANGE", key, end, start];
    if (typeof count === "number") {
      command.push("COUNT", count);
    }
    super(command, {
      deserialize: (result) => deserialize7(result),
      ...opts
    });
  }
};
function deserialize7(result) {
  const obj = {};
  for (const e of result) {
    for (let i = 0; i < e.length; i += 2) {
      const streamId = e[i];
      const entries = e[i + 1];
      if (!(streamId in obj)) {
        obj[streamId] = {};
      }
      for (let j = 0; j < entries.length; j += 2) {
        const field = entries[j];
        const value = entries[j + 1];
        try {
          obj[streamId][field] = JSON.parse(value);
        } catch {
          obj[streamId][field] = value;
        }
      }
    }
  }
  return obj;
}
var XTrimCommand = class extends Command {
  constructor([key, options], opts) {
    const { limit, strategy, threshold, exactness = "~" } = options;
    super(["XTRIM", key, strategy, exactness, threshold, ...limit ? ["LIMIT", limit] : []], opts);
  }
};
var ZAddCommand = class extends Command {
  constructor([key, arg1, ...arg2], opts) {
    const command = ["zadd", key];
    if ("nx" in arg1 && arg1.nx) {
      command.push("nx");
    } else if ("xx" in arg1 && arg1.xx) {
      command.push("xx");
    }
    if ("ch" in arg1 && arg1.ch) {
      command.push("ch");
    }
    if ("incr" in arg1 && arg1.incr) {
      command.push("incr");
    }
    if ("lt" in arg1 && arg1.lt) {
      command.push("lt");
    } else if ("gt" in arg1 && arg1.gt) {
      command.push("gt");
    }
    if ("score" in arg1 && "member" in arg1) {
      command.push(arg1.score, arg1.member);
    }
    command.push(...arg2.flatMap(({ score, member }) => [score, member]));
    super(command, opts);
  }
};
var ZCardCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zcard", ...cmd], opts);
  }
};
var ZCountCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zcount", ...cmd], opts);
  }
};
var ZIncrByCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zincrby", ...cmd], opts);
  }
};
var ZInterStoreCommand = class extends Command {
  constructor([destination, numKeys, keyOrKeys, opts], cmdOpts) {
    const command = ["zinterstore", destination, numKeys];
    if (Array.isArray(keyOrKeys)) {
      command.push(...keyOrKeys);
    } else {
      command.push(keyOrKeys);
    }
    if (opts) {
      if ("weights" in opts && opts.weights) {
        command.push("weights", ...opts.weights);
      } else if ("weight" in opts && typeof opts.weight === "number") {
        command.push("weights", opts.weight);
      }
      if ("aggregate" in opts) {
        command.push("aggregate", opts.aggregate);
      }
    }
    super(command, cmdOpts);
  }
};
var ZLexCountCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zlexcount", ...cmd], opts);
  }
};
var ZPopMaxCommand = class extends Command {
  constructor([key, count], opts) {
    const command = ["zpopmax", key];
    if (typeof count === "number") {
      command.push(count);
    }
    super(command, opts);
  }
};
var ZPopMinCommand = class extends Command {
  constructor([key, count], opts) {
    const command = ["zpopmin", key];
    if (typeof count === "number") {
      command.push(count);
    }
    super(command, opts);
  }
};
var ZRangeCommand = class extends Command {
  constructor([key, min, max, opts], cmdOpts) {
    const command = ["zrange", key, min, max];
    if (opts?.byScore) {
      command.push("byscore");
    }
    if (opts?.byLex) {
      command.push("bylex");
    }
    if (opts?.rev) {
      command.push("rev");
    }
    if (opts?.count !== void 0 && opts.offset !== void 0) {
      command.push("limit", opts.offset, opts.count);
    }
    if (opts?.withScores) {
      command.push("withscores");
    }
    super(command, cmdOpts);
  }
};
var ZRankCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zrank", ...cmd], opts);
  }
};
var ZRemCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zrem", ...cmd], opts);
  }
};
var ZRemRangeByLexCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zremrangebylex", ...cmd], opts);
  }
};
var ZRemRangeByRankCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zremrangebyrank", ...cmd], opts);
  }
};
var ZRemRangeByScoreCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zremrangebyscore", ...cmd], opts);
  }
};
var ZRevRankCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zrevrank", ...cmd], opts);
  }
};
var ZScanCommand = class extends Command {
  constructor([key, cursor, opts], cmdOpts) {
    const command = ["zscan", key, cursor];
    if (opts?.match) {
      command.push("match", opts.match);
    }
    if (typeof opts?.count === "number") {
      command.push("count", opts.count);
    }
    super(command, {
      deserialize: deserializeScanResponse,
      ...cmdOpts
    });
  }
};
var ZScoreCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zscore", ...cmd], opts);
  }
};
var ZUnionCommand = class extends Command {
  constructor([numKeys, keyOrKeys, opts], cmdOpts) {
    const command = ["zunion", numKeys];
    if (Array.isArray(keyOrKeys)) {
      command.push(...keyOrKeys);
    } else {
      command.push(keyOrKeys);
    }
    if (opts) {
      if ("weights" in opts && opts.weights) {
        command.push("weights", ...opts.weights);
      } else if ("weight" in opts && typeof opts.weight === "number") {
        command.push("weights", opts.weight);
      }
      if ("aggregate" in opts) {
        command.push("aggregate", opts.aggregate);
      }
      if (opts.withScores) {
        command.push("withscores");
      }
    }
    super(command, cmdOpts);
  }
};
var ZUnionStoreCommand = class extends Command {
  constructor([destination, numKeys, keyOrKeys, opts], cmdOpts) {
    const command = ["zunionstore", destination, numKeys];
    if (Array.isArray(keyOrKeys)) {
      command.push(...keyOrKeys);
    } else {
      command.push(keyOrKeys);
    }
    if (opts) {
      if ("weights" in opts && opts.weights) {
        command.push("weights", ...opts.weights);
      } else if ("weight" in opts && typeof opts.weight === "number") {
        command.push("weights", opts.weight);
      }
      if ("aggregate" in opts) {
        command.push("aggregate", opts.aggregate);
      }
    }
    super(command, cmdOpts);
  }
};
var ZDiffStoreCommand = class extends Command {
  constructor(cmd, opts) {
    super(["zdiffstore", ...cmd], opts);
  }
};
var ZMScoreCommand = class extends Command {
  constructor(cmd, opts) {
    const [key, members] = cmd;
    super(["zmscore", key, ...members], opts);
  }
};
var Pipeline = class {
  client;
  commands;
  commandOptions;
  multiExec;
  constructor(opts) {
    this.client = opts.client;
    this.commands = [];
    this.commandOptions = opts.commandOptions;
    this.multiExec = opts.multiExec ?? false;
    if (this.commandOptions?.latencyLogging) {
      const originalExec = this.exec.bind(this);
      this.exec = async (options) => {
        const start = performance.now();
        const result = await (options ? originalExec(options) : originalExec());
        const end = performance.now();
        const loggerResult = (end - start).toFixed(2);
        console.log(
          `Latency for \x1B[38;2;19;185;39m${this.multiExec ? ["MULTI-EXEC"] : ["PIPELINE"].toString().toUpperCase()}\x1B[0m: \x1B[38;2;0;255;255m${loggerResult} ms\x1B[0m`
        );
        return result;
      };
    }
  }
  exec = async (options) => {
    if (this.commands.length === 0) {
      throw new Error("Pipeline is empty");
    }
    const path = this.multiExec ? ["multi-exec"] : ["pipeline"];
    const res = await this.client.request({
      path,
      body: Object.values(this.commands).map((c) => c.command)
    });
    return options?.keepErrors ? res.map(({ error, result }, i) => {
      return {
        error,
        result: this.commands[i].deserialize(result)
      };
    }) : res.map(({ error, result }, i) => {
      if (error) {
        throw new UpstashError(
          `Command ${i + 1} [ ${this.commands[i].command[0]} ] failed: ${error}`
        );
      }
      return this.commands[i].deserialize(result);
    });
  };
  /**
   * Returns the length of pipeline before the execution
   */
  length() {
    return this.commands.length;
  }
  /**
   * Pushes a command into the pipeline and returns a chainable instance of the
   * pipeline
   */
  chain(command) {
    this.commands.push(command);
    return this;
  }
  /**
   * @see https://redis.io/commands/append
   */
  append = (...args) => this.chain(new AppendCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/bitcount
   */
  bitcount = (...args) => this.chain(new BitCountCommand(args, this.commandOptions));
  /**
   * Returns an instance that can be used to execute `BITFIELD` commands on one key.
   *
   * @example
   * ```typescript
   * redis.set("mykey", 0);
   * const result = await redis.pipeline()
   *   .bitfield("mykey")
   *   .set("u4", 0, 16)
   *   .incr("u4", "#1", 1)
   *   .exec();
   * console.log(result); // [[0, 1]]
   * ```
   *
   * @see https://redis.io/commands/bitfield
   */
  bitfield = (...args) => new BitFieldCommand(args, this.client, this.commandOptions, this.chain.bind(this));
  /**
   * @see https://redis.io/commands/bitop
   */
  bitop = (op, destinationKey, sourceKey, ...sourceKeys) => this.chain(
    new BitOpCommand([op, destinationKey, sourceKey, ...sourceKeys], this.commandOptions)
  );
  /**
   * @see https://redis.io/commands/bitpos
   */
  bitpos = (...args) => this.chain(new BitPosCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/copy
   */
  copy = (...args) => this.chain(new CopyCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zdiffstore
   */
  zdiffstore = (...args) => this.chain(new ZDiffStoreCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/dbsize
   */
  dbsize = () => this.chain(new DBSizeCommand(this.commandOptions));
  /**
   * @see https://redis.io/commands/decr
   */
  decr = (...args) => this.chain(new DecrCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/decrby
   */
  decrby = (...args) => this.chain(new DecrByCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/del
   */
  del = (...args) => this.chain(new DelCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/echo
   */
  echo = (...args) => this.chain(new EchoCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/eval_ro
   */
  evalRo = (...args) => this.chain(new EvalROCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/eval
   */
  eval = (...args) => this.chain(new EvalCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/evalsha_ro
   */
  evalshaRo = (...args) => this.chain(new EvalshaROCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/evalsha
   */
  evalsha = (...args) => this.chain(new EvalshaCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/exists
   */
  exists = (...args) => this.chain(new ExistsCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/expire
   */
  expire = (...args) => this.chain(new ExpireCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/expireat
   */
  expireat = (...args) => this.chain(new ExpireAtCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/flushall
   */
  flushall = (args) => this.chain(new FlushAllCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/flushdb
   */
  flushdb = (...args) => this.chain(new FlushDBCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/geoadd
   */
  geoadd = (...args) => this.chain(new GeoAddCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/geodist
   */
  geodist = (...args) => this.chain(new GeoDistCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/geopos
   */
  geopos = (...args) => this.chain(new GeoPosCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/geohash
   */
  geohash = (...args) => this.chain(new GeoHashCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/geosearch
   */
  geosearch = (...args) => this.chain(new GeoSearchCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/geosearchstore
   */
  geosearchstore = (...args) => this.chain(new GeoSearchStoreCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/get
   */
  get = (...args) => this.chain(new GetCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/getbit
   */
  getbit = (...args) => this.chain(new GetBitCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/getdel
   */
  getdel = (...args) => this.chain(new GetDelCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/getex
   */
  getex = (...args) => this.chain(new GetExCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/getrange
   */
  getrange = (...args) => this.chain(new GetRangeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/getset
   */
  getset = (key, value) => this.chain(new GetSetCommand([key, value], this.commandOptions));
  /**
   * @see https://redis.io/commands/hdel
   */
  hdel = (...args) => this.chain(new HDelCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hexists
   */
  hexists = (...args) => this.chain(new HExistsCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hexpire
   */
  hexpire = (...args) => this.chain(new HExpireCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hexpireat
   */
  hexpireat = (...args) => this.chain(new HExpireAtCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hexpiretime
   */
  hexpiretime = (...args) => this.chain(new HExpireTimeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/httl
   */
  httl = (...args) => this.chain(new HTtlCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hpexpire
   */
  hpexpire = (...args) => this.chain(new HPExpireCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hpexpireat
   */
  hpexpireat = (...args) => this.chain(new HPExpireAtCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hpexpiretime
   */
  hpexpiretime = (...args) => this.chain(new HPExpireTimeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hpttl
   */
  hpttl = (...args) => this.chain(new HPTtlCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hpersist
   */
  hpersist = (...args) => this.chain(new HPersistCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hget
   */
  hget = (...args) => this.chain(new HGetCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hgetall
   */
  hgetall = (...args) => this.chain(new HGetAllCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hincrby
   */
  hincrby = (...args) => this.chain(new HIncrByCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hincrbyfloat
   */
  hincrbyfloat = (...args) => this.chain(new HIncrByFloatCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hkeys
   */
  hkeys = (...args) => this.chain(new HKeysCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hlen
   */
  hlen = (...args) => this.chain(new HLenCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hmget
   */
  hmget = (...args) => this.chain(new HMGetCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hmset
   */
  hmset = (key, kv) => this.chain(new HMSetCommand([key, kv], this.commandOptions));
  /**
   * @see https://redis.io/commands/hrandfield
   */
  hrandfield = (key, count, withValues) => this.chain(new HRandFieldCommand([key, count, withValues], this.commandOptions));
  /**
   * @see https://redis.io/commands/hscan
   */
  hscan = (...args) => this.chain(new HScanCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hset
   */
  hset = (key, kv) => this.chain(new HSetCommand([key, kv], this.commandOptions));
  /**
   * @see https://redis.io/commands/hsetnx
   */
  hsetnx = (key, field, value) => this.chain(new HSetNXCommand([key, field, value], this.commandOptions));
  /**
   * @see https://redis.io/commands/hstrlen
   */
  hstrlen = (...args) => this.chain(new HStrLenCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/hvals
   */
  hvals = (...args) => this.chain(new HValsCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/incr
   */
  incr = (...args) => this.chain(new IncrCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/incrby
   */
  incrby = (...args) => this.chain(new IncrByCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/incrbyfloat
   */
  incrbyfloat = (...args) => this.chain(new IncrByFloatCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/keys
   */
  keys = (...args) => this.chain(new KeysCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/lindex
   */
  lindex = (...args) => this.chain(new LIndexCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/linsert
   */
  linsert = (key, direction, pivot, value) => this.chain(new LInsertCommand([key, direction, pivot, value], this.commandOptions));
  /**
   * @see https://redis.io/commands/llen
   */
  llen = (...args) => this.chain(new LLenCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/lmove
   */
  lmove = (...args) => this.chain(new LMoveCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/lpop
   */
  lpop = (...args) => this.chain(new LPopCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/lmpop
   */
  lmpop = (...args) => this.chain(new LmPopCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/lpos
   */
  lpos = (...args) => this.chain(new LPosCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/lpush
   */
  lpush = (key, ...elements) => this.chain(new LPushCommand([key, ...elements], this.commandOptions));
  /**
   * @see https://redis.io/commands/lpushx
   */
  lpushx = (key, ...elements) => this.chain(new LPushXCommand([key, ...elements], this.commandOptions));
  /**
   * @see https://redis.io/commands/lrange
   */
  lrange = (...args) => this.chain(new LRangeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/lrem
   */
  lrem = (key, count, value) => this.chain(new LRemCommand([key, count, value], this.commandOptions));
  /**
   * @see https://redis.io/commands/lset
   */
  lset = (key, index, value) => this.chain(new LSetCommand([key, index, value], this.commandOptions));
  /**
   * @see https://redis.io/commands/ltrim
   */
  ltrim = (...args) => this.chain(new LTrimCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/mget
   */
  mget = (...args) => this.chain(new MGetCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/mset
   */
  mset = (kv) => this.chain(new MSetCommand([kv], this.commandOptions));
  /**
   * @see https://redis.io/commands/msetnx
   */
  msetnx = (kv) => this.chain(new MSetNXCommand([kv], this.commandOptions));
  /**
   * @see https://redis.io/commands/persist
   */
  persist = (...args) => this.chain(new PersistCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/pexpire
   */
  pexpire = (...args) => this.chain(new PExpireCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/pexpireat
   */
  pexpireat = (...args) => this.chain(new PExpireAtCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/pfadd
   */
  pfadd = (...args) => this.chain(new PfAddCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/pfcount
   */
  pfcount = (...args) => this.chain(new PfCountCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/pfmerge
   */
  pfmerge = (...args) => this.chain(new PfMergeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/ping
   */
  ping = (args) => this.chain(new PingCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/psetex
   */
  psetex = (key, ttl, value) => this.chain(new PSetEXCommand([key, ttl, value], this.commandOptions));
  /**
   * @see https://redis.io/commands/pttl
   */
  pttl = (...args) => this.chain(new PTtlCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/publish
   */
  publish = (...args) => this.chain(new PublishCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/randomkey
   */
  randomkey = () => this.chain(new RandomKeyCommand(this.commandOptions));
  /**
   * @see https://redis.io/commands/rename
   */
  rename = (...args) => this.chain(new RenameCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/renamenx
   */
  renamenx = (...args) => this.chain(new RenameNXCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/rpop
   */
  rpop = (...args) => this.chain(new RPopCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/rpush
   */
  rpush = (key, ...elements) => this.chain(new RPushCommand([key, ...elements], this.commandOptions));
  /**
   * @see https://redis.io/commands/rpushx
   */
  rpushx = (key, ...elements) => this.chain(new RPushXCommand([key, ...elements], this.commandOptions));
  /**
   * @see https://redis.io/commands/sadd
   */
  sadd = (key, member, ...members) => this.chain(new SAddCommand([key, member, ...members], this.commandOptions));
  /**
   * @see https://redis.io/commands/scan
   */
  scan = (...args) => this.chain(new ScanCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/scard
   */
  scard = (...args) => this.chain(new SCardCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/script-exists
   */
  scriptExists = (...args) => this.chain(new ScriptExistsCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/script-flush
   */
  scriptFlush = (...args) => this.chain(new ScriptFlushCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/script-load
   */
  scriptLoad = (...args) => this.chain(new ScriptLoadCommand(args, this.commandOptions));
  /*)*
   * @see https://redis.io/commands/sdiff
   */
  sdiff = (...args) => this.chain(new SDiffCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/sdiffstore
   */
  sdiffstore = (...args) => this.chain(new SDiffStoreCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/set
   */
  set = (key, value, opts) => this.chain(new SetCommand([key, value, opts], this.commandOptions));
  /**
   * @see https://redis.io/commands/setbit
   */
  setbit = (...args) => this.chain(new SetBitCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/setex
   */
  setex = (key, ttl, value) => this.chain(new SetExCommand([key, ttl, value], this.commandOptions));
  /**
   * @see https://redis.io/commands/setnx
   */
  setnx = (key, value) => this.chain(new SetNxCommand([key, value], this.commandOptions));
  /**
   * @see https://redis.io/commands/setrange
   */
  setrange = (...args) => this.chain(new SetRangeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/sinter
   */
  sinter = (...args) => this.chain(new SInterCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/sinterstore
   */
  sinterstore = (...args) => this.chain(new SInterStoreCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/sismember
   */
  sismember = (key, member) => this.chain(new SIsMemberCommand([key, member], this.commandOptions));
  /**
   * @see https://redis.io/commands/smembers
   */
  smembers = (...args) => this.chain(new SMembersCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/smismember
   */
  smismember = (key, members) => this.chain(new SMIsMemberCommand([key, members], this.commandOptions));
  /**
   * @see https://redis.io/commands/smove
   */
  smove = (source, destination, member) => this.chain(new SMoveCommand([source, destination, member], this.commandOptions));
  /**
   * @see https://redis.io/commands/spop
   */
  spop = (...args) => this.chain(new SPopCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/srandmember
   */
  srandmember = (...args) => this.chain(new SRandMemberCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/srem
   */
  srem = (key, ...members) => this.chain(new SRemCommand([key, ...members], this.commandOptions));
  /**
   * @see https://redis.io/commands/sscan
   */
  sscan = (...args) => this.chain(new SScanCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/strlen
   */
  strlen = (...args) => this.chain(new StrLenCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/sunion
   */
  sunion = (...args) => this.chain(new SUnionCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/sunionstore
   */
  sunionstore = (...args) => this.chain(new SUnionStoreCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/time
   */
  time = () => this.chain(new TimeCommand(this.commandOptions));
  /**
   * @see https://redis.io/commands/touch
   */
  touch = (...args) => this.chain(new TouchCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/ttl
   */
  ttl = (...args) => this.chain(new TtlCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/type
   */
  type = (...args) => this.chain(new TypeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/unlink
   */
  unlink = (...args) => this.chain(new UnlinkCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zadd
   */
  zadd = (...args) => {
    if ("score" in args[1]) {
      return this.chain(
        new ZAddCommand([args[0], args[1], ...args.slice(2)], this.commandOptions)
      );
    }
    return this.chain(
      new ZAddCommand(
        [args[0], args[1], ...args.slice(2)],
        this.commandOptions
      )
    );
  };
  /**
   * @see https://redis.io/commands/xadd
   */
  xadd = (...args) => this.chain(new XAddCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xack
   */
  xack = (...args) => this.chain(new XAckCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xdel
   */
  xdel = (...args) => this.chain(new XDelCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xgroup
   */
  xgroup = (...args) => this.chain(new XGroupCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xread
   */
  xread = (...args) => this.chain(new XReadCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xreadgroup
   */
  xreadgroup = (...args) => this.chain(new XReadGroupCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xinfo
   */
  xinfo = (...args) => this.chain(new XInfoCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xlen
   */
  xlen = (...args) => this.chain(new XLenCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xpending
   */
  xpending = (...args) => this.chain(new XPendingCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xclaim
   */
  xclaim = (...args) => this.chain(new XClaimCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xautoclaim
   */
  xautoclaim = (...args) => this.chain(new XAutoClaim(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xtrim
   */
  xtrim = (...args) => this.chain(new XTrimCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xrange
   */
  xrange = (...args) => this.chain(new XRangeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/xrevrange
   */
  xrevrange = (...args) => this.chain(new XRevRangeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zcard
   */
  zcard = (...args) => this.chain(new ZCardCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zcount
   */
  zcount = (...args) => this.chain(new ZCountCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zincrby
   */
  zincrby = (key, increment, member) => this.chain(new ZIncrByCommand([key, increment, member], this.commandOptions));
  /**
   * @see https://redis.io/commands/zinterstore
   */
  zinterstore = (...args) => this.chain(new ZInterStoreCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zlexcount
   */
  zlexcount = (...args) => this.chain(new ZLexCountCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zmscore
   */
  zmscore = (...args) => this.chain(new ZMScoreCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zpopmax
   */
  zpopmax = (...args) => this.chain(new ZPopMaxCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zpopmin
   */
  zpopmin = (...args) => this.chain(new ZPopMinCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zrange
   */
  zrange = (...args) => this.chain(new ZRangeCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zrank
   */
  zrank = (key, member) => this.chain(new ZRankCommand([key, member], this.commandOptions));
  /**
   * @see https://redis.io/commands/zrem
   */
  zrem = (key, ...members) => this.chain(new ZRemCommand([key, ...members], this.commandOptions));
  /**
   * @see https://redis.io/commands/zremrangebylex
   */
  zremrangebylex = (...args) => this.chain(new ZRemRangeByLexCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zremrangebyrank
   */
  zremrangebyrank = (...args) => this.chain(new ZRemRangeByRankCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zremrangebyscore
   */
  zremrangebyscore = (...args) => this.chain(new ZRemRangeByScoreCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zrevrank
   */
  zrevrank = (key, member) => this.chain(new ZRevRankCommand([key, member], this.commandOptions));
  /**
   * @see https://redis.io/commands/zscan
   */
  zscan = (...args) => this.chain(new ZScanCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zscore
   */
  zscore = (key, member) => this.chain(new ZScoreCommand([key, member], this.commandOptions));
  /**
   * @see https://redis.io/commands/zunionstore
   */
  zunionstore = (...args) => this.chain(new ZUnionStoreCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/zunion
   */
  zunion = (...args) => this.chain(new ZUnionCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/?group=json
   */
  get json() {
    return {
      /**
       * @see https://redis.io/commands/json.arrappend
       */
      arrappend: (...args) => this.chain(new JsonArrAppendCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.arrindex
       */
      arrindex: (...args) => this.chain(new JsonArrIndexCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.arrinsert
       */
      arrinsert: (...args) => this.chain(new JsonArrInsertCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.arrlen
       */
      arrlen: (...args) => this.chain(new JsonArrLenCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.arrpop
       */
      arrpop: (...args) => this.chain(new JsonArrPopCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.arrtrim
       */
      arrtrim: (...args) => this.chain(new JsonArrTrimCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.clear
       */
      clear: (...args) => this.chain(new JsonClearCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.del
       */
      del: (...args) => this.chain(new JsonDelCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.forget
       */
      forget: (...args) => this.chain(new JsonForgetCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.get
       */
      get: (...args) => this.chain(new JsonGetCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.merge
       */
      merge: (...args) => this.chain(new JsonMergeCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.mget
       */
      mget: (...args) => this.chain(new JsonMGetCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.mset
       */
      mset: (...args) => this.chain(new JsonMSetCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.numincrby
       */
      numincrby: (...args) => this.chain(new JsonNumIncrByCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.nummultby
       */
      nummultby: (...args) => this.chain(new JsonNumMultByCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.objkeys
       */
      objkeys: (...args) => this.chain(new JsonObjKeysCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.objlen
       */
      objlen: (...args) => this.chain(new JsonObjLenCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.resp
       */
      resp: (...args) => this.chain(new JsonRespCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.set
       */
      set: (...args) => this.chain(new JsonSetCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.strappend
       */
      strappend: (...args) => this.chain(new JsonStrAppendCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.strlen
       */
      strlen: (...args) => this.chain(new JsonStrLenCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.toggle
       */
      toggle: (...args) => this.chain(new JsonToggleCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/commands/json.type
       */
      type: (...args) => this.chain(new JsonTypeCommand(args, this.commandOptions))
    };
  }
  get functions() {
    return {
      /**
       * @see https://redis.io/docs/latest/commands/function-load/
       */
      load: (...args) => this.chain(new FunctionLoadCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/docs/latest/commands/function-list/
       */
      list: (...args) => this.chain(new FunctionListCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/docs/latest/commands/function-delete/
       */
      delete: (...args) => this.chain(new FunctionDeleteCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/docs/latest/commands/function-flush/
       */
      flush: () => this.chain(new FunctionFlushCommand(this.commandOptions)),
      /**
       * @see https://redis.io/docs/latest/commands/function-stats/
       */
      stats: () => this.chain(new FunctionStatsCommand(this.commandOptions)),
      /**
       * @see https://redis.io/docs/latest/commands/fcall/
       */
      call: (...args) => this.chain(new FCallCommand(args, this.commandOptions)),
      /**
       * @see https://redis.io/docs/latest/commands/fcall_ro/
       */
      callRo: (...args) => this.chain(new FCallRoCommand(args, this.commandOptions))
    };
  }
};
var EXCLUDE_COMMANDS = /* @__PURE__ */ new Set([
  "scan",
  "keys",
  "flushdb",
  "flushall",
  "dbsize",
  "hscan",
  "hgetall",
  "hkeys",
  "lrange",
  "sscan",
  "smembers",
  "xrange",
  "xrevrange",
  "zscan",
  "zrange",
  "exec"
]);
function createAutoPipelineProxy(_redis, namespace = "root") {
  const redis = _redis;
  if (!redis.autoPipelineExecutor) {
    redis.autoPipelineExecutor = new AutoPipelineExecutor(redis);
  }
  return new Proxy(redis, {
    get: (redis2, command) => {
      if (command === "pipelineCounter") {
        return redis2.autoPipelineExecutor.pipelineCounter;
      }
      if (namespace === "root" && command === "json") {
        return createAutoPipelineProxy(redis2, "json");
      }
      if (namespace === "root" && command === "functions") {
        return createAutoPipelineProxy(redis2, "functions");
      }
      if (namespace === "root") {
        const commandInRedisButNotPipeline = command in redis2 && !(command in redis2.autoPipelineExecutor.pipeline);
        const isCommandExcluded = EXCLUDE_COMMANDS.has(command);
        if (commandInRedisButNotPipeline || isCommandExcluded) {
          return redis2[command];
        }
      }
      const pipeline = redis2.autoPipelineExecutor.pipeline;
      const targetFunction = namespace === "json" ? pipeline.json[command] : namespace === "functions" ? pipeline.functions[command] : pipeline[command];
      const isFunction = typeof targetFunction === "function";
      if (isFunction) {
        return (...args) => {
          return redis2.autoPipelineExecutor.withAutoPipeline((pipeline2) => {
            const targetFunction2 = namespace === "json" ? pipeline2.json[command] : namespace === "functions" ? pipeline2.functions[command] : pipeline2[command];
            targetFunction2(...args);
          });
        };
      }
      return targetFunction;
    }
  });
}
var AutoPipelineExecutor = class {
  pipelinePromises = /* @__PURE__ */ new WeakMap();
  activePipeline = null;
  indexInCurrentPipeline = 0;
  redis;
  pipeline;
  // only to make sure that proxy can work
  pipelineCounter = 0;
  // to keep track of how many times a pipeline was executed
  constructor(redis) {
    this.redis = redis;
    this.pipeline = redis.pipeline();
  }
  async withAutoPipeline(executeWithPipeline) {
    const pipeline = this.activePipeline ?? this.redis.pipeline();
    if (!this.activePipeline) {
      this.activePipeline = pipeline;
      this.indexInCurrentPipeline = 0;
    }
    const index = this.indexInCurrentPipeline++;
    executeWithPipeline(pipeline);
    const pipelineDone = this.deferExecution().then(() => {
      if (!this.pipelinePromises.has(pipeline)) {
        const pipelinePromise = pipeline.exec({ keepErrors: true });
        this.pipelineCounter += 1;
        this.pipelinePromises.set(pipeline, pipelinePromise);
        this.activePipeline = null;
      }
      return this.pipelinePromises.get(pipeline);
    });
    const results = await pipelineDone;
    const commandResult = results[index];
    if (commandResult.error) {
      throw new UpstashError(`Command failed: ${commandResult.error}`);
    }
    return commandResult.result;
  }
  async deferExecution() {
    await Promise.resolve();
    await Promise.resolve();
  }
};
var PSubscribeCommand = class extends Command {
  constructor(cmd, opts) {
    const sseHeaders = {
      Accept: "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive"
    };
    super([], {
      ...opts,
      headers: sseHeaders,
      path: ["psubscribe", ...cmd],
      streamOptions: {
        isStreaming: true,
        onMessage: opts?.streamOptions?.onMessage,
        signal: opts?.streamOptions?.signal
      }
    });
  }
};
var Subscriber = class extends EventTarget {
  subscriptions;
  client;
  listeners;
  opts;
  constructor(client, channels, isPattern = false, opts) {
    super();
    this.client = client;
    this.subscriptions = /* @__PURE__ */ new Map();
    this.listeners = /* @__PURE__ */ new Map();
    this.opts = opts;
    for (const channel2 of channels) {
      if (isPattern) {
        this.subscribeToPattern(channel2);
      } else {
        this.subscribeToChannel(channel2);
      }
    }
  }
  subscribeToChannel(channel2) {
    const controller = new AbortController();
    const command = new SubscribeCommand([channel2], {
      streamOptions: {
        signal: controller.signal,
        onMessage: (data) => this.handleMessage(data, false)
      }
    });
    command.exec(this.client).catch((error) => {
      if (error.name !== "AbortError") {
        this.dispatchToListeners("error", error);
      }
    });
    this.subscriptions.set(channel2, {
      command,
      controller,
      isPattern: false
    });
  }
  subscribeToPattern(pattern) {
    const controller = new AbortController();
    const command = new PSubscribeCommand([pattern], {
      streamOptions: {
        signal: controller.signal,
        onMessage: (data) => this.handleMessage(data, true)
      }
    });
    command.exec(this.client).catch((error) => {
      if (error.name !== "AbortError") {
        this.dispatchToListeners("error", error);
      }
    });
    this.subscriptions.set(pattern, {
      command,
      controller,
      isPattern: true
    });
  }
  handleMessage(data, isPattern) {
    const messageData = data.replace(/^data:\s*/, "");
    const firstCommaIndex = messageData.indexOf(",");
    const secondCommaIndex = messageData.indexOf(",", firstCommaIndex + 1);
    const thirdCommaIndex = isPattern ? messageData.indexOf(",", secondCommaIndex + 1) : -1;
    if (firstCommaIndex !== -1 && secondCommaIndex !== -1) {
      const type = messageData.slice(0, firstCommaIndex);
      if (isPattern && type === "pmessage" && thirdCommaIndex !== -1) {
        const pattern = messageData.slice(firstCommaIndex + 1, secondCommaIndex);
        const channel2 = messageData.slice(secondCommaIndex + 1, thirdCommaIndex);
        const messageStr = messageData.slice(thirdCommaIndex + 1);
        try {
          const message = this.opts?.automaticDeserialization === false ? messageStr : JSON.parse(messageStr);
          this.dispatchToListeners("pmessage", { pattern, channel: channel2, message });
          this.dispatchToListeners(`pmessage:${pattern}`, { pattern, channel: channel2, message });
        } catch (error) {
          this.dispatchToListeners("error", new Error(`Failed to parse message: ${error}`));
        }
      } else {
        const channel2 = messageData.slice(firstCommaIndex + 1, secondCommaIndex);
        const messageStr = messageData.slice(secondCommaIndex + 1);
        try {
          if (type === "subscribe" || type === "psubscribe" || type === "unsubscribe" || type === "punsubscribe") {
            const count = Number.parseInt(messageStr);
            this.dispatchToListeners(type, count);
          } else {
            const message = this.opts?.automaticDeserialization === false ? messageStr : parseWithTryCatch(messageStr);
            this.dispatchToListeners(type, { channel: channel2, message });
            this.dispatchToListeners(`${type}:${channel2}`, { channel: channel2, message });
          }
        } catch (error) {
          this.dispatchToListeners("error", new Error(`Failed to parse message: ${error}`));
        }
      }
    }
  }
  dispatchToListeners(type, data) {
    const listeners = this.listeners.get(type);
    if (listeners) {
      for (const listener of listeners) {
        listener(data);
      }
    }
  }
  on(type, listener) {
    if (!this.listeners.has(type)) {
      this.listeners.set(type, /* @__PURE__ */ new Set());
    }
    this.listeners.get(type)?.add(listener);
  }
  removeAllListeners() {
    this.listeners.clear();
  }
  async unsubscribe(channels) {
    if (channels) {
      for (const channel2 of channels) {
        const subscription = this.subscriptions.get(channel2);
        if (subscription) {
          try {
            subscription.controller.abort();
          } catch {
          }
          this.subscriptions.delete(channel2);
        }
      }
    } else {
      for (const subscription of this.subscriptions.values()) {
        try {
          subscription.controller.abort();
        } catch {
        }
      }
      this.subscriptions.clear();
      this.removeAllListeners();
    }
  }
  getSubscribedChannels() {
    return [...this.subscriptions.keys()];
  }
};
var SubscribeCommand = class extends Command {
  constructor(cmd, opts) {
    const sseHeaders = {
      Accept: "text/event-stream",
      "Cache-Control": "no-cache",
      Connection: "keep-alive"
    };
    super([], {
      ...opts,
      headers: sseHeaders,
      path: ["subscribe", ...cmd],
      streamOptions: {
        isStreaming: true,
        onMessage: opts?.streamOptions?.onMessage,
        signal: opts?.streamOptions?.signal
      }
    });
  }
};
var parseWithTryCatch = (str) => {
  try {
    return JSON.parse(str);
  } catch {
    return str;
  }
};
var Script = class {
  script;
  /**
   * @deprecated This property is initialized to an empty string and will be set in the init method
   * asynchronously. Do not use this property immidiately after the constructor.
   *
   * This property is only exposed for backwards compatibility and will be removed in the
   * future major release.
   */
  sha1;
  redis;
  constructor(redis, script) {
    this.redis = redis;
    this.script = script;
    this.sha1 = "";
    void this.init(script);
  }
  /**
   * Initialize the script by computing its SHA-1 hash.
   */
  async init(script) {
    if (this.sha1) return;
    this.sha1 = await this.digest(script);
  }
  /**
   * Send an `EVAL` command to redis.
   */
  async eval(keys, args) {
    await this.init(this.script);
    return await this.redis.eval(this.script, keys, args);
  }
  /**
   * Calculates the sha1 hash of the script and then calls `EVALSHA`.
   */
  async evalsha(keys, args) {
    await this.init(this.script);
    return await this.redis.evalsha(this.sha1, keys, args);
  }
  /**
   * Optimistically try to run `EVALSHA` first.
   * If the script is not loaded in redis, it will fall back and try again with `EVAL`.
   *
   * Following calls will be able to use the cached script
   */
  async exec(keys, args) {
    await this.init(this.script);
    const res = await this.redis.evalsha(this.sha1, keys, args).catch(async (error) => {
      if (error instanceof Error && error.message.toLowerCase().includes("noscript")) {
        return await this.redis.eval(this.script, keys, args);
      }
      throw error;
    });
    return res;
  }
  /**
   * Compute the sha1 hash of the script and return its hex representation.
   */
  async digest(s) {
    const data = new TextEncoder().encode(s);
    const hashBuffer = await subtle.digest("SHA-1", data);
    const hashArray = [...new Uint8Array(hashBuffer)];
    return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
  }
};
var ScriptRO = class {
  script;
  /**
   * @deprecated This property is initialized to an empty string and will be set in the init method
   * asynchronously. Do not use this property immidiately after the constructor.
   *
   * This property is only exposed for backwards compatibility and will be removed in the
   * future major release.
   */
  sha1;
  redis;
  constructor(redis, script) {
    this.redis = redis;
    this.sha1 = "";
    this.script = script;
    void this.init(script);
  }
  async init(script) {
    if (this.sha1) return;
    this.sha1 = await this.digest(script);
  }
  /**
   * Send an `EVAL_RO` command to redis.
   */
  async evalRo(keys, args) {
    await this.init(this.script);
    return await this.redis.evalRo(this.script, keys, args);
  }
  /**
   * Calculates the sha1 hash of the script and then calls `EVALSHA_RO`.
   */
  async evalshaRo(keys, args) {
    await this.init(this.script);
    return await this.redis.evalshaRo(this.sha1, keys, args);
  }
  /**
   * Optimistically try to run `EVALSHA_RO` first.
   * If the script is not loaded in redis, it will fall back and try again with `EVAL_RO`.
   *
   * Following calls will be able to use the cached script
   */
  async exec(keys, args) {
    await this.init(this.script);
    const res = await this.redis.evalshaRo(this.sha1, keys, args).catch(async (error) => {
      if (error instanceof Error && error.message.toLowerCase().includes("noscript")) {
        return await this.redis.evalRo(this.script, keys, args);
      }
      throw error;
    });
    return res;
  }
  /**
   * Compute the sha1 hash of the script and return its hex representation.
   */
  async digest(s) {
    const data = new TextEncoder().encode(s);
    const hashBuffer = await subtle.digest("SHA-1", data);
    const hashArray = [...new Uint8Array(hashBuffer)];
    return hashArray.map((b) => b.toString(16).padStart(2, "0")).join("");
  }
};
var Redis = class {
  client;
  opts;
  enableTelemetry;
  enableAutoPipelining;
  /**
   * Create a new redis client
   *
   * @example
   * ```typescript
   * const redis = new Redis({
   *  url: "<UPSTASH_REDIS_REST_URL>",
   *  token: "<UPSTASH_REDIS_REST_TOKEN>",
   * });
   * ```
   */
  constructor(client, opts) {
    this.client = client;
    this.opts = opts;
    this.enableTelemetry = opts?.enableTelemetry ?? true;
    if (opts?.readYourWrites === false) {
      this.client.readYourWrites = false;
    }
    this.enableAutoPipelining = opts?.enableAutoPipelining ?? true;
  }
  get readYourWritesSyncToken() {
    return this.client.upstashSyncToken;
  }
  set readYourWritesSyncToken(session) {
    this.client.upstashSyncToken = session;
  }
  get json() {
    return {
      /**
       * @see https://redis.io/commands/json.arrappend
       */
      arrappend: (...args) => new JsonArrAppendCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.arrindex
       */
      arrindex: (...args) => new JsonArrIndexCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.arrinsert
       */
      arrinsert: (...args) => new JsonArrInsertCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.arrlen
       */
      arrlen: (...args) => new JsonArrLenCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.arrpop
       */
      arrpop: (...args) => new JsonArrPopCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.arrtrim
       */
      arrtrim: (...args) => new JsonArrTrimCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.clear
       */
      clear: (...args) => new JsonClearCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.del
       */
      del: (...args) => new JsonDelCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.forget
       */
      forget: (...args) => new JsonForgetCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.get
       */
      get: (...args) => new JsonGetCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.merge
       */
      merge: (...args) => new JsonMergeCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.mget
       */
      mget: (...args) => new JsonMGetCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.mset
       */
      mset: (...args) => new JsonMSetCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.numincrby
       */
      numincrby: (...args) => new JsonNumIncrByCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.nummultby
       */
      nummultby: (...args) => new JsonNumMultByCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.objkeys
       */
      objkeys: (...args) => new JsonObjKeysCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.objlen
       */
      objlen: (...args) => new JsonObjLenCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.resp
       */
      resp: (...args) => new JsonRespCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.set
       */
      set: (...args) => new JsonSetCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.strappend
       */
      strappend: (...args) => new JsonStrAppendCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.strlen
       */
      strlen: (...args) => new JsonStrLenCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.toggle
       */
      toggle: (...args) => new JsonToggleCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/commands/json.type
       */
      type: (...args) => new JsonTypeCommand(args, this.opts).exec(this.client)
    };
  }
  get functions() {
    return {
      /**
       * @see https://redis.io/docs/latest/commands/function-load/
       */
      load: (...args) => new FunctionLoadCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/docs/latest/commands/function-list/
       */
      list: (...args) => new FunctionListCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/docs/latest/commands/function-delete/
       */
      delete: (...args) => new FunctionDeleteCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/docs/latest/commands/function-flush/
       */
      flush: () => new FunctionFlushCommand(this.opts).exec(this.client),
      /**
       * @see https://redis.io/docs/latest/commands/function-stats/
       *
       * Note: `running_script` field is not supported and therefore not included in the type.
       */
      stats: () => new FunctionStatsCommand(this.opts).exec(this.client),
      /**
       * @see https://redis.io/docs/latest/commands/fcall/
       */
      call: (...args) => new FCallCommand(args, this.opts).exec(this.client),
      /**
       * @see https://redis.io/docs/latest/commands/fcall_ro/
       */
      callRo: (...args) => new FCallRoCommand(args, this.opts).exec(this.client)
    };
  }
  /**
   * Wrap a new middleware around the HTTP client.
   */
  use = (middleware) => {
    const makeRequest = this.client.request.bind(this.client);
    this.client.request = (req) => middleware(req, makeRequest);
  };
  /**
   * Technically this is not private, we can hide it from intellisense by doing this
   */
  addTelemetry = (telemetry) => {
    if (!this.enableTelemetry) {
      return;
    }
    try {
      this.client.mergeTelemetry(telemetry);
    } catch {
    }
  };
  /**
   * Creates a new script.
   *
   * Scripts offer the ability to optimistically try to execute a script without having to send the
   * entire script to the server. If the script is loaded on the server, it tries again by sending
   * the entire script. Afterwards, the script is cached on the server.
   *
   * @param script - The script to create
   * @param opts - Optional options to pass to the script `{ readonly?: boolean }`
   * @returns A new script
   *
   * @example
   * ```ts
   * const redis = new Redis({...})
   *
   * const script = redis.createScript<string>("return ARGV[1];")
   * const arg1 = await script.eval([], ["Hello World"])
   * expect(arg1, "Hello World")
   * ```
   * @example
   * ```ts
   * const redis = new Redis({...})
   *
   * const script = redis.createScript<string>("return ARGV[1];", { readonly: true })
   * const arg1 = await script.evalRo([], ["Hello World"])
   * expect(arg1, "Hello World")
   * ```
   */
  createScript(script, opts) {
    return opts?.readonly ? new ScriptRO(this, script) : new Script(this, script);
  }
  /**
   * Create a new pipeline that allows you to send requests in bulk.
   *
   * @see {@link Pipeline}
   */
  pipeline = () => new Pipeline({
    client: this.client,
    commandOptions: this.opts,
    multiExec: false
  });
  autoPipeline = () => {
    return createAutoPipelineProxy(this);
  };
  /**
   * Create a new transaction to allow executing multiple steps atomically.
   *
   * All the commands in a transaction are serialized and executed sequentially. A request sent by
   * another client will never be served in the middle of the execution of a Redis Transaction. This
   * guarantees that the commands are executed as a single isolated operation.
   *
   * @see {@link Pipeline}
   */
  multi = () => new Pipeline({
    client: this.client,
    commandOptions: this.opts,
    multiExec: true
  });
  /**
   * Returns an instance that can be used to execute `BITFIELD` commands on one key.
   *
   * @example
   * ```typescript
   * redis.set("mykey", 0);
   * const result = await redis.bitfield("mykey")
   *   .set("u4", 0, 16)
   *   .incr("u4", "#1", 1)
   *   .exec();
   * console.log(result); // [0, 1]
   * ```
   *
   * @see https://redis.io/commands/bitfield
   */
  bitfield = (...args) => new BitFieldCommand(args, this.client, this.opts);
  /**
   * @see https://redis.io/commands/append
   */
  append = (...args) => new AppendCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/bitcount
   */
  bitcount = (...args) => new BitCountCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/bitop
   */
  bitop = (op, destinationKey, sourceKey, ...sourceKeys) => new BitOpCommand([op, destinationKey, sourceKey, ...sourceKeys], this.opts).exec(
    this.client
  );
  /**
   * @see https://redis.io/commands/bitpos
   */
  bitpos = (...args) => new BitPosCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/copy
   */
  copy = (...args) => new CopyCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/dbsize
   */
  dbsize = () => new DBSizeCommand(this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/decr
   */
  decr = (...args) => new DecrCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/decrby
   */
  decrby = (...args) => new DecrByCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/del
   */
  del = (...args) => new DelCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/echo
   */
  echo = (...args) => new EchoCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/eval_ro
   */
  evalRo = (...args) => new EvalROCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/eval
   */
  eval = (...args) => new EvalCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/evalsha_ro
   */
  evalshaRo = (...args) => new EvalshaROCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/evalsha
   */
  evalsha = (...args) => new EvalshaCommand(args, this.opts).exec(this.client);
  /**
   * Generic method to execute any Redis command.
   */
  exec = (args) => new ExecCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/exists
   */
  exists = (...args) => new ExistsCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/expire
   */
  expire = (...args) => new ExpireCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/expireat
   */
  expireat = (...args) => new ExpireAtCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/flushall
   */
  flushall = (args) => new FlushAllCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/flushdb
   */
  flushdb = (...args) => new FlushDBCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/geoadd
   */
  geoadd = (...args) => new GeoAddCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/geopos
   */
  geopos = (...args) => new GeoPosCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/geodist
   */
  geodist = (...args) => new GeoDistCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/geohash
   */
  geohash = (...args) => new GeoHashCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/geosearch
   */
  geosearch = (...args) => new GeoSearchCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/geosearchstore
   */
  geosearchstore = (...args) => new GeoSearchStoreCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/get
   */
  get = (...args) => new GetCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/getbit
   */
  getbit = (...args) => new GetBitCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/getdel
   */
  getdel = (...args) => new GetDelCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/getex
   */
  getex = (...args) => new GetExCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/getrange
   */
  getrange = (...args) => new GetRangeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/getset
   */
  getset = (key, value) => new GetSetCommand([key, value], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hdel
   */
  hdel = (...args) => new HDelCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hexists
   */
  hexists = (...args) => new HExistsCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hexpire
   */
  hexpire = (...args) => new HExpireCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hexpireat
   */
  hexpireat = (...args) => new HExpireAtCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hexpiretime
   */
  hexpiretime = (...args) => new HExpireTimeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/httl
   */
  httl = (...args) => new HTtlCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hpexpire
   */
  hpexpire = (...args) => new HPExpireCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hpexpireat
   */
  hpexpireat = (...args) => new HPExpireAtCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hpexpiretime
   */
  hpexpiretime = (...args) => new HPExpireTimeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hpttl
   */
  hpttl = (...args) => new HPTtlCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hpersist
   */
  hpersist = (...args) => new HPersistCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hget
   */
  hget = (...args) => new HGetCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hgetall
   */
  hgetall = (...args) => new HGetAllCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hincrby
   */
  hincrby = (...args) => new HIncrByCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hincrbyfloat
   */
  hincrbyfloat = (...args) => new HIncrByFloatCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hkeys
   */
  hkeys = (...args) => new HKeysCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hlen
   */
  hlen = (...args) => new HLenCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hmget
   */
  hmget = (...args) => new HMGetCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hmset
   */
  hmset = (key, kv) => new HMSetCommand([key, kv], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hrandfield
   */
  hrandfield = (key, count, withValues) => new HRandFieldCommand([key, count, withValues], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hscan
   */
  hscan = (...args) => new HScanCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hset
   */
  hset = (key, kv) => new HSetCommand([key, kv], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hsetnx
   */
  hsetnx = (key, field, value) => new HSetNXCommand([key, field, value], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hstrlen
   */
  hstrlen = (...args) => new HStrLenCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/hvals
   */
  hvals = (...args) => new HValsCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/incr
   */
  incr = (...args) => new IncrCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/incrby
   */
  incrby = (...args) => new IncrByCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/incrbyfloat
   */
  incrbyfloat = (...args) => new IncrByFloatCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/keys
   */
  keys = (...args) => new KeysCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lindex
   */
  lindex = (...args) => new LIndexCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/linsert
   */
  linsert = (key, direction, pivot, value) => new LInsertCommand([key, direction, pivot, value], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/llen
   */
  llen = (...args) => new LLenCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lmove
   */
  lmove = (...args) => new LMoveCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lpop
   */
  lpop = (...args) => new LPopCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lmpop
   */
  lmpop = (...args) => new LmPopCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lpos
   */
  lpos = (...args) => new LPosCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lpush
   */
  lpush = (key, ...elements) => new LPushCommand([key, ...elements], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lpushx
   */
  lpushx = (key, ...elements) => new LPushXCommand([key, ...elements], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lrange
   */
  lrange = (...args) => new LRangeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lrem
   */
  lrem = (key, count, value) => new LRemCommand([key, count, value], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/lset
   */
  lset = (key, index, value) => new LSetCommand([key, index, value], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/ltrim
   */
  ltrim = (...args) => new LTrimCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/mget
   */
  mget = (...args) => new MGetCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/mset
   */
  mset = (kv) => new MSetCommand([kv], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/msetnx
   */
  msetnx = (kv) => new MSetNXCommand([kv], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/persist
   */
  persist = (...args) => new PersistCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/pexpire
   */
  pexpire = (...args) => new PExpireCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/pexpireat
   */
  pexpireat = (...args) => new PExpireAtCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/pfadd
   */
  pfadd = (...args) => new PfAddCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/pfcount
   */
  pfcount = (...args) => new PfCountCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/pfmerge
   */
  pfmerge = (...args) => new PfMergeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/ping
   */
  ping = (args) => new PingCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/psetex
   */
  psetex = (key, ttl, value) => new PSetEXCommand([key, ttl, value], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/psubscribe
   */
  psubscribe = (patterns) => {
    const patternArray = Array.isArray(patterns) ? patterns : [patterns];
    return new Subscriber(this.client, patternArray, true, this.opts);
  };
  /**
   * @see https://redis.io/commands/pttl
   */
  pttl = (...args) => new PTtlCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/publish
   */
  publish = (...args) => new PublishCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/randomkey
   */
  randomkey = () => new RandomKeyCommand().exec(this.client);
  /**
   * @see https://redis.io/commands/rename
   */
  rename = (...args) => new RenameCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/renamenx
   */
  renamenx = (...args) => new RenameNXCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/rpop
   */
  rpop = (...args) => new RPopCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/rpush
   */
  rpush = (key, ...elements) => new RPushCommand([key, ...elements], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/rpushx
   */
  rpushx = (key, ...elements) => new RPushXCommand([key, ...elements], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/sadd
   */
  sadd = (key, member, ...members) => new SAddCommand([key, member, ...members], this.opts).exec(this.client);
  scan(cursor, opts) {
    return new ScanCommand([cursor, opts], this.opts).exec(this.client);
  }
  /**
   * @see https://redis.io/commands/scard
   */
  scard = (...args) => new SCardCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/script-exists
   */
  scriptExists = (...args) => new ScriptExistsCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/script-flush
   */
  scriptFlush = (...args) => new ScriptFlushCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/script-load
   */
  scriptLoad = (...args) => new ScriptLoadCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/sdiff
   */
  sdiff = (...args) => new SDiffCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/sdiffstore
   */
  sdiffstore = (...args) => new SDiffStoreCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/set
   */
  set = (key, value, opts) => new SetCommand([key, value, opts], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/setbit
   */
  setbit = (...args) => new SetBitCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/setex
   */
  setex = (key, ttl, value) => new SetExCommand([key, ttl, value], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/setnx
   */
  setnx = (key, value) => new SetNxCommand([key, value], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/setrange
   */
  setrange = (...args) => new SetRangeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/sinter
   */
  sinter = (...args) => new SInterCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/sinterstore
   */
  sinterstore = (...args) => new SInterStoreCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/sismember
   */
  sismember = (key, member) => new SIsMemberCommand([key, member], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/smismember
   */
  smismember = (key, members) => new SMIsMemberCommand([key, members], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/smembers
   */
  smembers = (...args) => new SMembersCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/smove
   */
  smove = (source, destination, member) => new SMoveCommand([source, destination, member], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/spop
   */
  spop = (...args) => new SPopCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/srandmember
   */
  srandmember = (...args) => new SRandMemberCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/srem
   */
  srem = (key, ...members) => new SRemCommand([key, ...members], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/sscan
   */
  sscan = (...args) => new SScanCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/strlen
   */
  strlen = (...args) => new StrLenCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/subscribe
   */
  subscribe = (channels) => {
    const channelArray = Array.isArray(channels) ? channels : [channels];
    return new Subscriber(this.client, channelArray, false, this.opts);
  };
  /**
   * @see https://redis.io/commands/sunion
   */
  sunion = (...args) => new SUnionCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/sunionstore
   */
  sunionstore = (...args) => new SUnionStoreCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/time
   */
  time = () => new TimeCommand().exec(this.client);
  /**
   * @see https://redis.io/commands/touch
   */
  touch = (...args) => new TouchCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/ttl
   */
  ttl = (...args) => new TtlCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/type
   */
  type = (...args) => new TypeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/unlink
   */
  unlink = (...args) => new UnlinkCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xadd
   */
  xadd = (...args) => new XAddCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xack
   */
  xack = (...args) => new XAckCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xdel
   */
  xdel = (...args) => new XDelCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xgroup
   */
  xgroup = (...args) => new XGroupCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xread
   */
  xread = (...args) => new XReadCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xreadgroup
   */
  xreadgroup = (...args) => new XReadGroupCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xinfo
   */
  xinfo = (...args) => new XInfoCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xlen
   */
  xlen = (...args) => new XLenCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xpending
   */
  xpending = (...args) => new XPendingCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xclaim
   */
  xclaim = (...args) => new XClaimCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xautoclaim
   */
  xautoclaim = (...args) => new XAutoClaim(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xtrim
   */
  xtrim = (...args) => new XTrimCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xrange
   */
  xrange = (...args) => new XRangeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/xrevrange
   */
  xrevrange = (...args) => new XRevRangeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zadd
   */
  zadd = (...args) => {
    if ("score" in args[1]) {
      return new ZAddCommand([args[0], args[1], ...args.slice(2)], this.opts).exec(
        this.client
      );
    }
    return new ZAddCommand(
      [args[0], args[1], ...args.slice(2)],
      this.opts
    ).exec(this.client);
  };
  /**
   * @see https://redis.io/commands/zcard
   */
  zcard = (...args) => new ZCardCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zcount
   */
  zcount = (...args) => new ZCountCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zdiffstore
   */
  zdiffstore = (...args) => new ZDiffStoreCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zincrby
   */
  zincrby = (key, increment, member) => new ZIncrByCommand([key, increment, member], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zinterstore
   */
  zinterstore = (...args) => new ZInterStoreCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zlexcount
   */
  zlexcount = (...args) => new ZLexCountCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zmscore
   */
  zmscore = (...args) => new ZMScoreCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zpopmax
   */
  zpopmax = (...args) => new ZPopMaxCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zpopmin
   */
  zpopmin = (...args) => new ZPopMinCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zrange
   */
  zrange = (...args) => new ZRangeCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zrank
   */
  zrank = (key, member) => new ZRankCommand([key, member], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zrem
   */
  zrem = (key, ...members) => new ZRemCommand([key, ...members], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zremrangebylex
   */
  zremrangebylex = (...args) => new ZRemRangeByLexCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zremrangebyrank
   */
  zremrangebyrank = (...args) => new ZRemRangeByRankCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zremrangebyscore
   */
  zremrangebyscore = (...args) => new ZRemRangeByScoreCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zrevrank
   */
  zrevrank = (key, member) => new ZRevRankCommand([key, member], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zscan
   */
  zscan = (...args) => new ZScanCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zscore
   */
  zscore = (key, member) => new ZScoreCommand([key, member], this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zunion
   */
  zunion = (...args) => new ZUnionCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/zunionstore
   */
  zunionstore = (...args) => new ZUnionStoreCommand(args, this.opts).exec(this.client);
};
var VERSION = "v1.36.1";

// node_modules/@upstash/redis/nodejs.mjs
if (typeof atob === "undefined") {
  global.atob = (b64) => Buffer.from(b64, "base64").toString("utf8");
}
var Redis2 = class _Redis extends Redis {
  /**
   * Create a new redis client by providing a custom `Requester` implementation
   *
   * @example
   * ```ts
   *
   * import { UpstashRequest, Requester, UpstashResponse, Redis } from "@upstash/redis"
   *
   *  const requester: Requester = {
   *    request: <TResult>(req: UpstashRequest): Promise<UpstashResponse<TResult>> => {
   *      // ...
   *    }
   *  }
   *
   * const redis = new Redis(requester)
   * ```
   */
  constructor(configOrRequester) {
    if ("request" in configOrRequester) {
      super(configOrRequester);
      return;
    }
    if (!configOrRequester.url) {
      console.warn(
        `[Upstash Redis] The 'url' property is missing or undefined in your Redis config.`
      );
    } else if (configOrRequester.url.startsWith(" ") || configOrRequester.url.endsWith(" ") || /\r|\n/.test(configOrRequester.url)) {
      console.warn(
        "[Upstash Redis] The redis url contains whitespace or newline, which can cause errors!"
      );
    }
    if (!configOrRequester.token) {
      console.warn(
        `[Upstash Redis] The 'token' property is missing or undefined in your Redis config.`
      );
    } else if (configOrRequester.token.startsWith(" ") || configOrRequester.token.endsWith(" ") || /\r|\n/.test(configOrRequester.token)) {
      console.warn(
        "[Upstash Redis] The redis token contains whitespace or newline, which can cause errors!"
      );
    }
    const client = new HttpClient({
      baseUrl: configOrRequester.url,
      retry: configOrRequester.retry,
      headers: { authorization: `Bearer ${configOrRequester.token}` },
      agent: configOrRequester.agent,
      responseEncoding: configOrRequester.responseEncoding,
      cache: configOrRequester.cache ?? "no-store",
      signal: configOrRequester.signal,
      keepAlive: configOrRequester.keepAlive,
      readYourWrites: configOrRequester.readYourWrites
    });
    const safeEnv = typeof process === "object" && process && typeof process.env === "object" && process.env ? process.env : {};
    super(client, {
      automaticDeserialization: configOrRequester.automaticDeserialization,
      enableTelemetry: configOrRequester.enableTelemetry ?? !safeEnv.UPSTASH_DISABLE_TELEMETRY,
      latencyLogging: configOrRequester.latencyLogging,
      enableAutoPipelining: configOrRequester.enableAutoPipelining
    });
    const nodeVersion = typeof process === "object" && process ? process.version : void 0;
    this.addTelemetry({
      runtime: (
        // @ts-expect-error to silence compiler
        typeof EdgeRuntime === "string" ? "edge-light" : nodeVersion ? `node@${nodeVersion}` : "unknown"
      ),
      platform: safeEnv.UPSTASH_CONSOLE ? "console" : safeEnv.VERCEL ? "vercel" : safeEnv.AWS_REGION ? "aws" : "unknown",
      sdk: `@upstash/redis@${VERSION}`
    });
    if (this.enableAutoPipelining) {
      return this.autoPipeline();
    }
  }
  /**
   * Create a new Upstash Redis instance from environment variables.
   *
   * Use this to automatically load connection secrets from your environment
   * variables. For instance when using the Vercel integration.
   *
   * This tries to load connection details from your environment using `process.env`:
   * - URL: `UPSTASH_REDIS_REST_URL` or fallback to `KV_REST_API_URL`
   * - Token: `UPSTASH_REDIS_REST_TOKEN` or fallback to `KV_REST_API_TOKEN`
   *
   * The fallback variables provide compatibility with Vercel KV and other platforms
   * that may use different naming conventions.
   */
  static fromEnv(config2) {
    if (typeof process !== "object" || !process || typeof process.env !== "object" || !process.env) {
      throw new TypeError(
        '[Upstash Redis] Unable to get environment variables, `process.env` is undefined. If you are deploying to cloudflare, please import from "@upstash/redis/cloudflare" instead'
      );
    }
    const url = process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL;
    if (!url) {
      console.warn("[Upstash Redis] Unable to find environment variable: `UPSTASH_REDIS_REST_URL`");
    }
    const token = process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN;
    if (!token) {
      console.warn(
        "[Upstash Redis] Unable to find environment variable: `UPSTASH_REDIS_REST_TOKEN`"
      );
    }
    return new _Redis({ ...config2, url, token });
  }
};

// server/_shared/rate-limit.ts
var ratelimit = null;
function getRatelimit() {
  if (ratelimit) return ratelimit;
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  ratelimit = new import_ratelimit.Ratelimit({
    redis: new Redis2({ url, token }),
    limiter: import_ratelimit.Ratelimit.slidingWindow(300, "60 s"),
    prefix: "rl",
    analytics: false
  });
  return ratelimit;
}
function getClientIp(request) {
  return request.headers.get("x-real-ip") || request.headers.get("cf-connecting-ip") || request.headers.get("x-forwarded-for")?.split(",")[0]?.trim() || "0.0.0.0";
}
async function checkRateLimit(request, corsHeaders) {
  const rl = getRatelimit();
  if (!rl) return null;
  const ip = getClientIp(request);
  try {
    const { success, limit, reset } = await rl.limit(ip);
    if (!success) {
      return new Response(JSON.stringify({ error: "Too many requests" }), {
        status: 429,
        headers: {
          "Content-Type": "application/json",
          "X-RateLimit-Limit": String(limit),
          "X-RateLimit-Remaining": "0",
          "X-RateLimit-Reset": String(reset),
          "Retry-After": String(Math.ceil((reset - Date.now()) / 1e3)),
          ...corsHeaders
        }
      });
    }
    return null;
  } catch {
    return null;
  }
}

// server/_shared/response-headers.ts
var channel = /* @__PURE__ */ new WeakMap();
function setResponseHeader(req, key, value) {
  let headers = channel.get(req);
  if (!headers) {
    headers = {};
    channel.set(req, headers);
  }
  headers[key] = value;
}
function markNoCacheResponse(req) {
  setResponseHeader(req, "X-No-Cache", "1");
}
function drainResponseHeaders(req) {
  const headers = channel.get(req);
  if (headers) channel.delete(req);
  return headers;
}

// src/generated/server/worldmonitor/seismology/v1/service_server.ts
var ValidationError = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createSeismologyServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/seismology/v1/list-earthquakes",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            start: Number(params.get("start") ?? "0"),
            end: Number(params.get("end") ?? "0"),
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            minMagnitude: Number(params.get("min_magnitude") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listEarthquakes", body);
            if (bodyViolations) {
              throw new ValidationError(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listEarthquakes(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/_shared/redis.ts
function getKeyPrefix() {
  const env = process.env.VERCEL_ENV;
  if (!env || env === "production") return "";
  const sha = process.env.VERCEL_GIT_COMMIT_SHA?.slice(0, 8) || "dev";
  return `${env}:${sha}:`;
}
var cachedPrefix;
function prefixKey(key) {
  if (cachedPrefix === void 0) cachedPrefix = getKeyPrefix();
  if (!cachedPrefix) return key;
  return `${cachedPrefix}${key}`;
}
async function getCachedJson(key, raw = false) {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return null;
  try {
    const finalKey = raw ? key : prefixKey(key);
    const resp = await fetch(`${url}/get/${encodeURIComponent(finalKey)}`, {
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(3e3)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    return data.result ? JSON.parse(data.result) : null;
  } catch {
    return null;
  }
}
async function setCachedJson(key, value, ttlSeconds) {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return;
  try {
    await fetch(`${url}/set/${encodeURIComponent(prefixKey(key))}/${encodeURIComponent(JSON.stringify(value))}/EX/${ttlSeconds}`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}` },
      signal: AbortSignal.timeout(3e3)
    });
  } catch {
  }
}
var NEG_SENTINEL = "__WM_NEG__";
async function getCachedJsonBatch(keys) {
  const result = /* @__PURE__ */ new Map();
  if (keys.length === 0) return result;
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return result;
  try {
    const pipeline = keys.map((k) => ["GET", prefixKey(k)]);
    const resp = await fetch(`${url}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify(pipeline),
      signal: AbortSignal.timeout(3e3)
    });
    if (!resp.ok) return result;
    const data = await resp.json();
    for (let i = 0; i < keys.length; i++) {
      const raw = data[i]?.result;
      if (raw) {
        try {
          const parsed = JSON.parse(raw);
          if (parsed !== NEG_SENTINEL) result.set(keys[i], parsed);
        } catch {
        }
      }
    }
  } catch {
  }
  return result;
}
var inflight = /* @__PURE__ */ new Map();
async function cachedFetchJson(key, ttlSeconds, fetcher, negativeTtlSeconds = 120) {
  const cached = await getCachedJson(key);
  if (cached === NEG_SENTINEL) return null;
  if (cached !== null) return cached;
  const existing = inflight.get(key);
  if (existing) return existing;
  const promise = fetcher().then(async (result) => {
    if (result != null) {
      await setCachedJson(key, result, ttlSeconds);
    } else {
      await setCachedJson(key, NEG_SENTINEL, negativeTtlSeconds);
    }
    return result;
  }).finally(() => {
    inflight.delete(key);
  });
  inflight.set(key, promise);
  return promise;
}
async function cachedFetchJsonWithMeta(key, ttlSeconds, fetcher, negativeTtlSeconds = 120) {
  const cached = await getCachedJson(key);
  if (cached === NEG_SENTINEL) return { data: null, source: "cache" };
  if (cached !== null) return { data: cached, source: "cache" };
  const existing = inflight.get(key);
  if (existing) {
    const data2 = await existing;
    return { data: data2, source: "fresh" };
  }
  const promise = fetcher().then(async (result) => {
    if (result != null) {
      await setCachedJson(key, result, ttlSeconds);
    } else {
      await setCachedJson(key, NEG_SENTINEL, negativeTtlSeconds);
    }
    return result;
  }).finally(() => {
    inflight.delete(key);
  });
  inflight.set(key, promise);
  const data = await promise;
  return { data, source: "fresh" };
}
async function geoSearchByBox(key, lon, lat, widthKm, heightKm, count, raw = false) {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return [];
  try {
    const finalKey = raw ? key : prefixKey(key);
    const pipeline = [
      [
        "GEOSEARCH",
        finalKey,
        "FROMLONLAT",
        String(lon),
        String(lat),
        "BYBOX",
        String(widthKm),
        String(heightKm),
        "km",
        "ASC",
        "COUNT",
        String(count)
      ]
    ];
    const resp = await fetch(`${url}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify(pipeline),
      signal: AbortSignal.timeout(5e3)
    });
    if (!resp.ok) return [];
    const data = await resp.json();
    return data[0]?.result ?? [];
  } catch {
    return [];
  }
}
async function getHashFieldsBatch(key, fields, raw = false) {
  const result = /* @__PURE__ */ new Map();
  if (fields.length === 0) return result;
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return result;
  try {
    const finalKey = raw ? key : prefixKey(key);
    const pipeline = [["HMGET", finalKey, ...fields]];
    const resp = await fetch(`${url}/pipeline`, {
      method: "POST",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify(pipeline),
      signal: AbortSignal.timeout(5e3)
    });
    if (!resp.ok) return result;
    const data = await resp.json();
    const values = data[0]?.result;
    if (values) {
      for (let i = 0; i < fields.length; i++) {
        if (values[i]) result.set(fields[i], values[i]);
      }
    }
  } catch {
  }
  return result;
}

// server/_shared/constants.ts
var CHROME_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36";
var yahooLastRequest = 0;
var YAHOO_MIN_GAP_MS = 600;
var yahooQueue = Promise.resolve();
function yahooGate() {
  yahooQueue = yahooQueue.then(async () => {
    const elapsed = Date.now() - yahooLastRequest;
    if (elapsed < YAHOO_MIN_GAP_MS) {
      await new Promise((r) => setTimeout(r, YAHOO_MIN_GAP_MS - elapsed));
    }
    yahooLastRequest = Date.now();
  });
  return yahooQueue;
}

// server/worldmonitor/seismology/v1/list-earthquakes.ts
var USGS_FEED_URL = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/4.5_day.geojson";
var CACHE_KEY = "seismology:earthquakes:v1";
var CACHE_TTL = 1800;
var listEarthquakes = async (_ctx, req) => {
  const pageSize = req.pageSize || 500;
  try {
    const cached = await cachedFetchJson(CACHE_KEY, CACHE_TTL, async () => {
      const response = await fetch(USGS_FEED_URL, {
        headers: { Accept: "application/json", "User-Agent": CHROME_UA },
        signal: AbortSignal.timeout(1e4)
      });
      if (!response.ok) {
        throw new Error(`USGS API error: ${response.status}`);
      }
      const geojson = await response.json();
      const features = geojson.features || [];
      const earthquakes2 = features.filter((f) => f?.properties && f?.geometry?.coordinates).map((f) => ({
        id: f.id || "",
        place: f.properties?.place || "",
        magnitude: f.properties?.mag ?? 0,
        depthKm: f.geometry?.coordinates?.[2] ?? 0,
        location: {
          latitude: f.geometry?.coordinates?.[1] ?? 0,
          longitude: f.geometry?.coordinates?.[0] ?? 0
        },
        occurredAt: f.properties?.time ?? 0,
        sourceUrl: f.properties?.url || ""
      }));
      return earthquakes2.length > 0 ? { earthquakes: earthquakes2 } : null;
    });
    const earthquakes = cached?.earthquakes || [];
    return { earthquakes: earthquakes.slice(0, pageSize), pagination: void 0 };
  } catch {
    return { earthquakes: [], pagination: void 0 };
  }
};

// server/worldmonitor/seismology/v1/handler.ts
var seismologyHandler = {
  listEarthquakes
};

// src/generated/server/worldmonitor/wildfire/v1/service_server.ts
var ValidationError2 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createWildfireServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/wildfire/v1/list-fire-detections",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            start: Number(params.get("start") ?? "0"),
            end: Number(params.get("end") ?? "0"),
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            neLat: Number(params.get("ne_lat") ?? "0"),
            neLon: Number(params.get("ne_lon") ?? "0"),
            swLat: Number(params.get("sw_lat") ?? "0"),
            swLon: Number(params.get("sw_lon") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listFireDetections", body);
            if (bodyViolations) {
              throw new ValidationError2(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listFireDetections(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError2) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/wildfire/v1/list-fire-detections.ts
var REDIS_CACHE_KEY = "wildfire:fires:v1";
var REDIS_CACHE_TTL = 3600;
var FIRMS_SOURCE = "VIIRS_SNPP_NRT";
var MONITORED_REGIONS = {
  "Ukraine": "22,44,40,53",
  "Russia": "20,50,180,82",
  "Iran": "44,25,63,40",
  "Israel/Gaza": "34,29,36,34",
  "Syria": "35,32,42,37",
  "Taiwan": "119,21,123,26",
  "North Korea": "124,37,131,43",
  "Saudi Arabia": "34,16,56,32",
  "Turkey": "26,36,45,42"
};
function mapConfidence(c) {
  switch (c.toLowerCase()) {
    case "h":
      return "FIRE_CONFIDENCE_HIGH";
    case "n":
      return "FIRE_CONFIDENCE_NOMINAL";
    case "l":
      return "FIRE_CONFIDENCE_LOW";
    default:
      return "FIRE_CONFIDENCE_UNSPECIFIED";
  }
}
function parseCSV(csv) {
  const lines = csv.trim().split("\n");
  if (lines.length < 2) return [];
  const headers = lines[0].split(",").map((h) => h.trim());
  const results = [];
  for (let i = 1; i < lines.length; i++) {
    const vals = lines[i].split(",").map((v) => v.trim());
    if (vals.length < headers.length) continue;
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = vals[idx];
    });
    results.push(row);
  }
  return results;
}
function parseDetectedAt(acqDate, acqTime) {
  const padded = acqTime.padStart(4, "0");
  const hours = padded.slice(0, 2);
  const minutes = padded.slice(2);
  return (/* @__PURE__ */ new Date(`${acqDate}T${hours}:${minutes}:00Z`)).getTime();
}
var listFireDetections = async (_ctx, _req) => {
  const apiKey = process.env.NASA_FIRMS_API_KEY || process.env.FIRMS_API_KEY || "";
  if (!apiKey) {
    return { fireDetections: [], pagination: void 0 };
  }
  let result = null;
  try {
    result = await cachedFetchJson(
      REDIS_CACHE_KEY,
      REDIS_CACHE_TTL,
      async () => {
        const entries = Object.entries(MONITORED_REGIONS);
        const results = await Promise.allSettled(
          entries.map(async ([regionName, bbox]) => {
            const url = `https://firms.modaps.eosdis.nasa.gov/api/area/csv/${apiKey}/${FIRMS_SOURCE}/${bbox}/1`;
            const res = await fetch(url, {
              headers: { Accept: "text/csv", "User-Agent": CHROME_UA },
              signal: AbortSignal.timeout(15e3)
            });
            if (!res.ok) {
              throw new Error(`FIRMS ${res.status} for ${regionName}`);
            }
            const csv = await res.text();
            const rows = parseCSV(csv);
            return { regionName, rows };
          })
        );
        const fireDetections = [];
        for (const r of results) {
          if (r.status === "fulfilled") {
            const { regionName, rows } = r.value;
            for (const row of rows) {
              const detectedAt = parseDetectedAt(row.acq_date || "", row.acq_time || "");
              fireDetections.push({
                id: `${row.latitude ?? ""}-${row.longitude ?? ""}-${row.acq_date ?? ""}-${row.acq_time ?? ""}`,
                location: {
                  latitude: parseFloat(row.latitude ?? "0") || 0,
                  longitude: parseFloat(row.longitude ?? "0") || 0
                },
                brightness: parseFloat(row.bright_ti4 ?? "0") || 0,
                frp: parseFloat(row.frp ?? "0") || 0,
                confidence: mapConfidence(row.confidence || ""),
                satellite: row.satellite || "",
                detectedAt,
                region: regionName,
                dayNight: row.daynight || ""
              });
            }
          } else {
            console.error("[FIRMS]", r.reason?.message);
          }
        }
        return fireDetections.length > 0 ? { fireDetections, pagination: void 0 } : null;
      }
    );
  } catch {
    return { fireDetections: [], pagination: void 0 };
  }
  return result || { fireDetections: [], pagination: void 0 };
};

// server/worldmonitor/wildfire/v1/handler.ts
var wildfireHandler = {
  listFireDetections
};

// src/generated/server/worldmonitor/climate/v1/service_server.ts
var ValidationError3 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createClimateServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/climate/v1/list-climate-anomalies",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            minSeverity: params.get("min_severity") ?? "ANOMALY_SEVERITY_UNSPECIFIED"
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listClimateAnomalies", body);
            if (bodyViolations) {
              throw new ValidationError3(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listClimateAnomalies(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError3) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/climate/v1/list-climate-anomalies.ts
var REDIS_CACHE_KEY2 = "climate:anomalies:v1";
var REDIS_CACHE_TTL2 = 10800;
var ZONES = [
  { name: "Ukraine", lat: 48.4, lon: 31.2 },
  { name: "Middle East", lat: 33, lon: 44 },
  { name: "Sahel", lat: 14, lon: 0 },
  { name: "Horn of Africa", lat: 8, lon: 42 },
  { name: "South Asia", lat: 25, lon: 78 },
  { name: "California", lat: 36.8, lon: -119.4 },
  { name: "Amazon", lat: -3.4, lon: -60 },
  { name: "Australia", lat: -25, lon: 134 },
  { name: "Mediterranean", lat: 38, lon: 20 },
  { name: "Taiwan Strait", lat: 24, lon: 120 },
  { name: "Myanmar", lat: 19.8, lon: 96.7 },
  { name: "Central Africa", lat: 4, lon: 22 },
  { name: "Southern Africa", lat: -25, lon: 28 },
  { name: "Central Asia", lat: 42, lon: 65 },
  { name: "Caribbean", lat: 19, lon: -72 }
];
function classifySeverity(tempDelta, precipDelta) {
  const absTemp = Math.abs(tempDelta);
  const absPrecip = Math.abs(precipDelta);
  if (absTemp >= 5 || absPrecip >= 80) return "ANOMALY_SEVERITY_EXTREME";
  if (absTemp >= 3 || absPrecip >= 40) return "ANOMALY_SEVERITY_MODERATE";
  return "ANOMALY_SEVERITY_NORMAL";
}
function classifyType(tempDelta, precipDelta) {
  const absTemp = Math.abs(tempDelta);
  const absPrecip = Math.abs(precipDelta);
  if (absTemp >= absPrecip / 20) {
    if (tempDelta > 0 && precipDelta < -20) return "ANOMALY_TYPE_MIXED";
    if (tempDelta > 3) return "ANOMALY_TYPE_WARM";
    if (tempDelta < -3) return "ANOMALY_TYPE_COLD";
  }
  if (precipDelta > 40) return "ANOMALY_TYPE_WET";
  if (precipDelta < -40) return "ANOMALY_TYPE_DRY";
  if (tempDelta > 0) return "ANOMALY_TYPE_WARM";
  return "ANOMALY_TYPE_COLD";
}
function avg(arr) {
  return arr.length ? arr.reduce((s, v) => s + v, 0) / arr.length : 0;
}
async function fetchZone(zone, startDate, endDate) {
  const url = `https://archive-api.open-meteo.com/v1/archive?latitude=${zone.lat}&longitude=${zone.lon}&start_date=${startDate}&end_date=${endDate}&daily=temperature_2m_mean,precipitation_sum&timezone=UTC`;
  const response = await fetch(url, { headers: { "User-Agent": CHROME_UA }, signal: AbortSignal.timeout(2e4) });
  if (!response.ok) {
    throw new Error(`Open-Meteo ${response.status} for ${zone.name}`);
  }
  const data = await response.json();
  const rawTemps = data.daily?.temperature_2m_mean ?? [];
  const rawPrecips = data.daily?.precipitation_sum ?? [];
  const temps = [];
  const precips = [];
  for (let i = 0; i < rawTemps.length; i++) {
    if (rawTemps[i] != null && rawPrecips[i] != null) {
      temps.push(rawTemps[i]);
      precips.push(rawPrecips[i]);
    }
  }
  if (temps.length < 14) return null;
  const recentTemps = temps.slice(-7);
  const baselineTemps = temps.slice(0, -7);
  const recentPrecips = precips.slice(-7);
  const baselinePrecips = precips.slice(0, -7);
  const tempDelta = Math.round((avg(recentTemps) - avg(baselineTemps)) * 10) / 10;
  const precipDelta = Math.round((avg(recentPrecips) - avg(baselinePrecips)) * 10) / 10;
  return {
    zone: zone.name,
    location: { latitude: zone.lat, longitude: zone.lon },
    tempDelta,
    precipDelta,
    severity: classifySeverity(tempDelta, precipDelta),
    type: classifyType(tempDelta, precipDelta),
    period: `${startDate} to ${endDate}`
  };
}
var listClimateAnomalies = async (_ctx, _req) => {
  let result = null;
  try {
    result = await cachedFetchJson(
      REDIS_CACHE_KEY2,
      REDIS_CACHE_TTL2,
      async () => {
        const endDate = (/* @__PURE__ */ new Date()).toISOString().slice(0, 10);
        const startDate = new Date(Date.now() - 30 * 24 * 60 * 60 * 1e3).toISOString().slice(0, 10);
        const results = await Promise.allSettled(
          ZONES.map((zone) => fetchZone(zone, startDate, endDate))
        );
        const anomalies = [];
        for (const r of results) {
          if (r.status === "fulfilled") {
            if (r.value != null) anomalies.push(r.value);
          } else {
            console.error("[CLIMATE]", r.reason?.message ?? r.reason);
          }
        }
        return anomalies.length > 0 ? { anomalies, pagination: void 0 } : null;
      }
    );
  } catch {
    return { anomalies: [], pagination: void 0 };
  }
  return result || { anomalies: [], pagination: void 0 };
};

// server/worldmonitor/climate/v1/handler.ts
var climateHandler = {
  listClimateAnomalies
};

// src/generated/server/worldmonitor/prediction/v1/service_server.ts
var ValidationError4 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createPredictionServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/prediction/v1/list-prediction-markets",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            category: params.get("category") ?? "",
            query: params.get("query") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listPredictionMarkets", body);
            if (bodyViolations) {
              throw new ValidationError4(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listPredictionMarkets(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError4) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/prediction/v1/list-prediction-markets.ts
var REDIS_CACHE_KEY3 = "prediction:markets:v1";
var REDIS_CACHE_TTL3 = 600;
var GAMMA_BASE = "https://gamma-api.polymarket.com";
var FETCH_TIMEOUT = 8e3;
function parseYesPrice(market) {
  try {
    const pricesStr = market.outcomePrices;
    if (pricesStr) {
      const prices = JSON.parse(pricesStr);
      if (prices.length >= 1) {
        const parsed = parseFloat(prices[0]);
        if (!isNaN(parsed)) return parsed;
      }
    }
  } catch {
  }
  return 0.5;
}
function mapEvent(event, category) {
  const topMarket = event.markets?.[0];
  const endDateStr = topMarket?.endDate ?? event.endDate;
  const closesAtMs = endDateStr ? Date.parse(endDateStr) : 0;
  return {
    id: event.id || "",
    title: topMarket?.question || event.title,
    yesPrice: topMarket ? parseYesPrice(topMarket) : 0.5,
    volume: event.volume ?? 0,
    url: `https://polymarket.com/event/${event.slug}`,
    closesAt: Number.isFinite(closesAtMs) ? closesAtMs : 0,
    category: category || ""
  };
}
function mapMarket(market) {
  const closesAtMs = market.endDate ? Date.parse(market.endDate) : 0;
  return {
    id: market.slug || "",
    title: market.question,
    yesPrice: parseYesPrice(market),
    volume: (market.volumeNum ?? (market.volume ? parseFloat(market.volume) : 0)) || 0,
    url: `https://polymarket.com/market/${market.slug}`,
    closesAt: Number.isFinite(closesAtMs) ? closesAtMs : 0,
    category: ""
  };
}
var listPredictionMarkets = async (_ctx, req) => {
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY3}:${req.category || "all"}:${req.query || ""}:${req.pageSize || 50}`;
    const result = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL3,
      async () => {
        const useEvents = !!req.category;
        const endpoint = useEvents ? "events" : "markets";
        const limit = Math.max(1, Math.min(100, req.pageSize || 50));
        const params = new URLSearchParams({
          closed: "false",
          active: "true",
          archived: "false",
          end_date_min: (/* @__PURE__ */ new Date()).toISOString(),
          order: "volume",
          ascending: "false",
          limit: String(limit)
        });
        if (useEvents) {
          params.set("tag_slug", req.category);
        }
        const response = await fetch(
          `${GAMMA_BASE}/${endpoint}?${params}`,
          {
            headers: { Accept: "application/json", "User-Agent": CHROME_UA },
            signal: AbortSignal.timeout(FETCH_TIMEOUT)
          }
        );
        if (!response.ok) return null;
        const data = await response.json();
        let markets;
        if (useEvents) {
          markets = data.map((e) => mapEvent(e, req.category));
        } else {
          markets = data.map(mapMarket);
        }
        if (req.query) {
          const q = req.query.toLowerCase();
          markets = markets.filter((m) => m.title.toLowerCase().includes(q));
        }
        return markets.length > 0 ? { markets, pagination: void 0 } : null;
      }
    );
    return result || { markets: [], pagination: void 0 };
  } catch {
    return { markets: [], pagination: void 0 };
  }
};

// server/worldmonitor/prediction/v1/handler.ts
var predictionHandler = {
  listPredictionMarkets
};

// src/generated/server/worldmonitor/displacement/v1/service_server.ts
var ValidationError5 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createDisplacementServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/displacement/v1/get-displacement-summary",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            year: Number(params.get("year") ?? "0"),
            countryLimit: Number(params.get("country_limit") ?? "0"),
            flowLimit: Number(params.get("flow_limit") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getDisplacementSummary", body);
            if (bodyViolations) {
              throw new ValidationError5(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getDisplacementSummary(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError5) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/displacement/v1/get-population-exposure",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            mode: params.get("mode") ?? "",
            lat: Number(params.get("lat") ?? "0"),
            lon: Number(params.get("lon") ?? "0"),
            radius: Number(params.get("radius") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getPopulationExposure", body);
            if (bodyViolations) {
              throw new ValidationError5(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getPopulationExposure(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError5) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/displacement/v1/get-displacement-summary.ts
var REDIS_CACHE_KEY4 = "displacement:summary:v1";
var REDIS_CACHE_TTL4 = 43200;
var COUNTRY_CENTROIDS = {
  AFG: [33.9, 67.7],
  SYR: [35, 38],
  UKR: [48.4, 31.2],
  SDN: [15.5, 32.5],
  SSD: [6.9, 31.3],
  SOM: [5.2, 46.2],
  COD: [-4, 21.8],
  MMR: [19.8, 96.7],
  YEM: [15.6, 48.5],
  ETH: [9.1, 40.5],
  VEN: [6.4, -66.6],
  IRQ: [33.2, 43.7],
  COL: [4.6, -74.1],
  NGA: [9.1, 7.5],
  PSE: [31.9, 35.2],
  TUR: [39.9, 32.9],
  DEU: [51.2, 10.4],
  PAK: [30.4, 69.3],
  UGA: [1.4, 32.3],
  BGD: [23.7, 90.4],
  KEN: [0, 38],
  TCD: [15.5, 19],
  JOR: [31, 36],
  LBN: [33.9, 35.5],
  EGY: [26.8, 30.8],
  IRN: [32.4, 53.7],
  TZA: [-6.4, 34.9],
  RWA: [-1.9, 29.9],
  CMR: [7.4, 12.4],
  MLI: [17.6, -4],
  BFA: [12.3, -1.6],
  NER: [17.6, 8.1],
  CAF: [6.6, 20.9],
  MOZ: [-18.7, 35.5],
  USA: [37.1, -95.7],
  FRA: [46.2, 2.2],
  GBR: [55.4, -3.4],
  IND: [20.6, 79],
  CHN: [35.9, 104.2],
  RUS: [61.5, 105.3]
};
async function fetchUnhcrYearItems(year) {
  const limit = 1e4;
  const maxPageGuard = 25;
  const items = [];
  for (let page = 1; page <= maxPageGuard; page++) {
    const response = await fetch(
      `https://api.unhcr.org/population/v1/population/?year=${year}&limit=${limit}&page=${page}`,
      { headers: { Accept: "application/json", "User-Agent": CHROME_UA } }
    );
    if (!response.ok) return null;
    const data = await response.json();
    const pageItems = Array.isArray(data.items) ? data.items : [];
    if (pageItems.length === 0) break;
    items.push(...pageItems);
    const maxPages = Number(data.maxPages);
    if (Number.isFinite(maxPages) && maxPages > 0) {
      if (page >= maxPages) break;
      continue;
    }
    if (pageItems.length < limit) break;
  }
  return items;
}
function getCoordinates(code) {
  const centroid = COUNTRY_CENTROIDS[code];
  if (!centroid) return void 0;
  return { latitude: centroid[0], longitude: centroid[1] };
}
async function getDisplacementSummary(_ctx, req) {
  const emptyResponse = {
    summary: {
      year: req.year > 0 ? req.year : (/* @__PURE__ */ new Date()).getFullYear(),
      globalTotals: { refugees: 0, asylumSeekers: 0, idps: 0, stateless: 0, total: 0 },
      countries: [],
      topFlows: []
    }
  };
  try {
    const year = req.year > 0 ? req.year : (/* @__PURE__ */ new Date()).getFullYear();
    const cacheKey2 = `${REDIS_CACHE_KEY4}:${year}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL4, async () => {
      const currentYear = (/* @__PURE__ */ new Date()).getFullYear();
      const requestYear = req.year > 0 ? req.year : 0;
      let rawItems = [];
      let dataYearUsed = currentYear;
      if (requestYear > 0) {
        const items = await fetchUnhcrYearItems(requestYear);
        if (items && items.length > 0) {
          rawItems = items;
          dataYearUsed = requestYear;
        }
      } else {
        for (let y = currentYear; y >= currentYear - 2; y--) {
          const items = await fetchUnhcrYearItems(y);
          if (!items) continue;
          if (items.length > 0) {
            rawItems = items;
            dataYearUsed = y;
            break;
          }
        }
      }
      if (rawItems.length === 0) return null;
      const byOrigin = {};
      const byAsylum = {};
      const flowMap = {};
      let totalRefugees = 0;
      let totalAsylumSeekers = 0;
      let totalIdps = 0;
      let totalStateless = 0;
      for (const item of rawItems) {
        const originCode = item.coo_iso || "";
        const asylumCode = item.coa_iso || "";
        const refugees = Number(item.refugees) || 0;
        const asylumSeekers = Number(item.asylum_seekers) || 0;
        const idps = Number(item.idps) || 0;
        const stateless = Number(item.stateless) || 0;
        totalRefugees += refugees;
        totalAsylumSeekers += asylumSeekers;
        totalIdps += idps;
        totalStateless += stateless;
        if (originCode) {
          if (!byOrigin[originCode]) {
            byOrigin[originCode] = {
              name: item.coo_name || originCode,
              refugees: 0,
              asylumSeekers: 0,
              idps: 0,
              stateless: 0
            };
          }
          byOrigin[originCode].refugees += refugees;
          byOrigin[originCode].asylumSeekers += asylumSeekers;
          byOrigin[originCode].idps += idps;
          byOrigin[originCode].stateless += stateless;
        }
        if (asylumCode) {
          if (!byAsylum[asylumCode]) {
            byAsylum[asylumCode] = {
              name: item.coa_name || asylumCode,
              refugees: 0,
              asylumSeekers: 0
            };
          }
          byAsylum[asylumCode].refugees += refugees;
          byAsylum[asylumCode].asylumSeekers += asylumSeekers;
        }
        if (originCode && asylumCode && refugees > 0) {
          const flowKey = `${originCode}->${asylumCode}`;
          if (!flowMap[flowKey]) {
            flowMap[flowKey] = {
              originCode,
              originName: item.coo_name || originCode,
              asylumCode,
              asylumName: item.coa_name || asylumCode,
              refugees: 0
            };
          }
          flowMap[flowKey].refugees += refugees;
        }
      }
      const countries = {};
      for (const [code, data] of Object.entries(byOrigin)) {
        countries[code] = {
          code,
          name: data.name,
          refugees: data.refugees,
          asylumSeekers: data.asylumSeekers,
          idps: data.idps,
          stateless: data.stateless,
          totalDisplaced: data.refugees + data.asylumSeekers + data.idps + data.stateless,
          hostRefugees: 0,
          hostAsylumSeekers: 0,
          hostTotal: 0
        };
      }
      for (const [code, data] of Object.entries(byAsylum)) {
        const hostRefugees = data.refugees;
        const hostAsylumSeekers = data.asylumSeekers;
        const hostTotal = hostRefugees + hostAsylumSeekers;
        if (!countries[code]) {
          countries[code] = {
            code,
            name: data.name,
            refugees: 0,
            asylumSeekers: 0,
            idps: 0,
            stateless: 0,
            totalDisplaced: 0,
            hostRefugees,
            hostAsylumSeekers,
            hostTotal
          };
        } else {
          countries[code].hostRefugees = hostRefugees;
          countries[code].hostAsylumSeekers = hostAsylumSeekers;
          countries[code].hostTotal = hostTotal;
        }
      }
      const sortedCountries = Object.values(countries).sort((a, b) => {
        const aSize = Math.max(a.totalDisplaced, a.hostTotal);
        const bSize = Math.max(b.totalDisplaced, b.hostTotal);
        return bSize - aSize;
      });
      const protoCountries = sortedCountries.map((d) => ({
        code: d.code,
        name: d.name,
        refugees: d.refugees,
        asylumSeekers: d.asylumSeekers,
        idps: d.idps,
        stateless: d.stateless,
        totalDisplaced: d.totalDisplaced,
        hostRefugees: d.hostRefugees,
        hostAsylumSeekers: d.hostAsylumSeekers,
        hostTotal: d.hostTotal,
        location: getCoordinates(d.code)
      }));
      const protoFlows = Object.values(flowMap).sort((a, b) => b.refugees - a.refugees).map((f) => ({
        originCode: f.originCode,
        originName: f.originName,
        asylumCode: f.asylumCode,
        asylumName: f.asylumName,
        refugees: f.refugees,
        originLocation: getCoordinates(f.originCode),
        asylumLocation: getCoordinates(f.asylumCode)
      }));
      return {
        summary: {
          year: dataYearUsed,
          globalTotals: {
            refugees: totalRefugees,
            asylumSeekers: totalAsylumSeekers,
            idps: totalIdps,
            stateless: totalStateless,
            total: totalRefugees + totalAsylumSeekers + totalIdps + totalStateless
          },
          countries: protoCountries,
          topFlows: protoFlows
        }
      };
    });
    if (result?.summary) {
      const summary = { ...result.summary };
      if (req.countryLimit > 0) {
        summary.countries = summary.countries.slice(0, req.countryLimit);
      }
      const flowLimit = req.flowLimit > 0 ? req.flowLimit : 50;
      summary.topFlows = summary.topFlows.slice(0, flowLimit);
      return { summary };
    }
    return result || emptyResponse;
  } catch {
    return emptyResponse;
  }
}

// server/worldmonitor/displacement/v1/get-population-exposure.ts
var PRIORITY_COUNTRIES = {
  UKR: { name: "Ukraine", pop: 37e6, area: 603550 },
  RUS: { name: "Russia", pop: 1441e5, area: 17098242 },
  ISR: { name: "Israel", pop: 98e5, area: 22072 },
  PSE: { name: "Palestine", pop: 54e5, area: 6020 },
  SYR: { name: "Syria", pop: 221e5, area: 185180 },
  IRN: { name: "Iran", pop: 886e5, area: 1648195 },
  TWN: { name: "Taiwan", pop: 236e5, area: 36193 },
  ETH: { name: "Ethiopia", pop: 1265e5, area: 1104300 },
  SDN: { name: "Sudan", pop: 481e5, area: 1861484 },
  SSD: { name: "South Sudan", pop: 114e5, area: 619745 },
  SOM: { name: "Somalia", pop: 181e5, area: 637657 },
  YEM: { name: "Yemen", pop: 344e5, area: 527968 },
  AFG: { name: "Afghanistan", pop: 422e5, area: 652230 },
  PAK: { name: "Pakistan", pop: 2405e5, area: 881913 },
  IND: { name: "India", pop: 14286e5, area: 3287263 },
  MMR: { name: "Myanmar", pop: 542e5, area: 676578 },
  COD: { name: "DR Congo", pop: 1023e5, area: 2344858 },
  NGA: { name: "Nigeria", pop: 2238e5, area: 923768 },
  MLI: { name: "Mali", pop: 226e5, area: 1240192 },
  BFA: { name: "Burkina Faso", pop: 227e5, area: 274200 }
};
var EXPOSURE_CENTROIDS = {
  UKR: [48.4, 31.2],
  RUS: [61.5, 105.3],
  ISR: [31, 34.8],
  PSE: [31.9, 35.2],
  SYR: [35, 38],
  IRN: [32.4, 53.7],
  TWN: [23.7, 121],
  ETH: [9.1, 40.5],
  SDN: [15.5, 32.5],
  SSD: [6.9, 31.3],
  SOM: [5.2, 46.2],
  YEM: [15.6, 48.5],
  AFG: [33.9, 67.7],
  PAK: [30.4, 69.3],
  IND: [20.6, 79],
  MMR: [19.8, 96.7],
  COD: [-4, 21.8],
  NGA: [9.1, 7.5],
  MLI: [17.6, -4],
  BFA: [12.3, -1.6]
};
async function getPopulationExposure(_ctx, req) {
  if (req.mode === "exposure") {
    const { lat, lon } = req;
    const radius = req.radius || 50;
    let bestMatch = null;
    let bestDist = Infinity;
    for (const [code, [cLat, cLon]] of Object.entries(EXPOSURE_CENTROIDS)) {
      const dist = Math.sqrt(Math.pow(lat - cLat, 2) + Math.pow(lon - cLon, 2));
      if (dist < bestDist) {
        bestDist = dist;
        bestMatch = code;
      }
    }
    const info = bestMatch ? PRIORITY_COUNTRIES[bestMatch] : { pop: 5e7, area: 5e5 };
    const density = info.pop / info.area;
    const areaKm2 = Math.PI * radius * radius;
    const exposed = Math.round(density * areaKm2);
    return {
      success: true,
      countries: [],
      exposure: {
        exposedPopulation: exposed,
        exposureRadiusKm: radius,
        nearestCountry: bestMatch || "",
        densityPerKm2: Math.round(density)
      }
    };
  }
  const countries = Object.entries(PRIORITY_COUNTRIES).map(([code, info]) => ({
    code,
    name: info.name,
    population: info.pop,
    densityPerKm2: Math.round(info.pop / info.area)
  }));
  return { success: true, countries };
}

// server/worldmonitor/displacement/v1/handler.ts
var displacementHandler = {
  getDisplacementSummary,
  getPopulationExposure
};

// src/generated/server/worldmonitor/aviation/v1/service_server.ts
var ValidationError6 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createAviationServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/aviation/v1/list-airport-delays",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            region: params.get("region") ?? "AIRPORT_REGION_UNSPECIFIED",
            minSeverity: params.get("min_severity") ?? "FLIGHT_DELAY_SEVERITY_UNSPECIFIED"
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listAirportDelays", body);
            if (bodyViolations) {
              throw new ValidationError6(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listAirportDelays(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError6) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// src/config/airports.ts
var MONITORED_AIRPORTS = [
  // Americas - Major US Hubs
  { iata: "JFK", icao: "KJFK", name: "John F. Kennedy International", city: "New York", country: "USA", lat: 40.6413, lon: -73.7781, region: "americas" },
  { iata: "LAX", icao: "KLAX", name: "Los Angeles International", city: "Los Angeles", country: "USA", lat: 33.9416, lon: -118.4085, region: "americas" },
  { iata: "ORD", icao: "KORD", name: "O'Hare International", city: "Chicago", country: "USA", lat: 41.9742, lon: -87.9073, region: "americas" },
  { iata: "ATL", icao: "KATL", name: "Hartsfield-Jackson Atlanta", city: "Atlanta", country: "USA", lat: 33.6407, lon: -84.4277, region: "americas" },
  { iata: "DFW", icao: "KDFW", name: "Dallas/Fort Worth International", city: "Dallas", country: "USA", lat: 32.8998, lon: -97.0403, region: "americas" },
  { iata: "DEN", icao: "KDEN", name: "Denver International", city: "Denver", country: "USA", lat: 39.8561, lon: -104.6737, region: "americas" },
  { iata: "SFO", icao: "KSFO", name: "San Francisco International", city: "San Francisco", country: "USA", lat: 37.6213, lon: -122.379, region: "americas" },
  { iata: "SEA", icao: "KSEA", name: "Seattle-Tacoma International", city: "Seattle", country: "USA", lat: 47.4502, lon: -122.3088, region: "americas" },
  { iata: "MIA", icao: "KMIA", name: "Miami International", city: "Miami", country: "USA", lat: 25.7959, lon: -80.287, region: "americas" },
  { iata: "BOS", icao: "KBOS", name: "Boston Logan International", city: "Boston", country: "USA", lat: 42.3656, lon: -71.0096, region: "americas" },
  { iata: "EWR", icao: "KEWR", name: "Newark Liberty International", city: "Newark", country: "USA", lat: 40.6895, lon: -74.1745, region: "americas" },
  { iata: "IAH", icao: "KIAH", name: "George Bush Intercontinental", city: "Houston", country: "USA", lat: 29.9902, lon: -95.3368, region: "americas" },
  { iata: "PHX", icao: "KPHX", name: "Phoenix Sky Harbor", city: "Phoenix", country: "USA", lat: 33.4373, lon: -112.0078, region: "americas" },
  { iata: "LAS", icao: "KLAS", name: "Harry Reid International", city: "Las Vegas", country: "USA", lat: 36.084, lon: -115.1537, region: "americas" },
  // Americas - Other
  { iata: "YYZ", icao: "CYYZ", name: "Toronto Pearson", city: "Toronto", country: "Canada", lat: 43.6777, lon: -79.6248, region: "americas" },
  { iata: "YVR", icao: "CYVR", name: "Vancouver International", city: "Vancouver", country: "Canada", lat: 49.1947, lon: -123.1792, region: "americas" },
  { iata: "MEX", icao: "MMMX", name: "Mexico City International", city: "Mexico City", country: "Mexico", lat: 19.4363, lon: -99.0721, region: "americas" },
  { iata: "GRU", icao: "SBGR", name: "S\xE3o Paulo\u2013Guarulhos", city: "S\xE3o Paulo", country: "Brazil", lat: -23.4356, lon: -46.4731, region: "americas" },
  { iata: "EZE", icao: "SAEZ", name: "Ministro Pistarini", city: "Buenos Aires", country: "Argentina", lat: -34.8222, lon: -58.5358, region: "americas" },
  { iata: "BOG", icao: "SKBO", name: "El Dorado International", city: "Bogot\xE1", country: "Colombia", lat: 4.7016, lon: -74.1469, region: "americas" },
  { iata: "SCL", icao: "SCEL", name: "Arturo Merino Ben\xEDtez", city: "Santiago", country: "Chile", lat: -33.393, lon: -70.7858, region: "americas" },
  { iata: "LIM", icao: "SPJC", name: "Jorge Ch\xE1vez International", city: "Lima", country: "Peru", lat: -12.0219, lon: -77.1143, region: "americas" },
  // Europe - Major Hubs
  { iata: "LHR", icao: "EGLL", name: "London Heathrow", city: "London", country: "UK", lat: 51.47, lon: -0.4543, region: "europe" },
  { iata: "CDG", icao: "LFPG", name: "Paris Charles de Gaulle", city: "Paris", country: "France", lat: 49.0097, lon: 2.5479, region: "europe" },
  { iata: "FRA", icao: "EDDF", name: "Frankfurt Airport", city: "Frankfurt", country: "Germany", lat: 50.0379, lon: 8.5622, region: "europe" },
  { iata: "AMS", icao: "EHAM", name: "Amsterdam Schiphol", city: "Amsterdam", country: "Netherlands", lat: 52.3105, lon: 4.7683, region: "europe" },
  { iata: "MAD", icao: "LEMD", name: "Adolfo Su\xE1rez Madrid\u2013Barajas", city: "Madrid", country: "Spain", lat: 40.4983, lon: -3.5676, region: "europe" },
  { iata: "FCO", icao: "LIRF", name: "Leonardo da Vinci\u2013Fiumicino", city: "Rome", country: "Italy", lat: 41.8003, lon: 12.2389, region: "europe" },
  { iata: "MUC", icao: "EDDM", name: "Munich Airport", city: "Munich", country: "Germany", lat: 48.3537, lon: 11.775, region: "europe" },
  { iata: "BCN", icao: "LEBL", name: "Barcelona\u2013El Prat", city: "Barcelona", country: "Spain", lat: 41.2974, lon: 2.0833, region: "europe" },
  { iata: "LGW", icao: "EGKK", name: "London Gatwick", city: "London", country: "UK", lat: 51.1537, lon: -0.1821, region: "europe" },
  { iata: "ZRH", icao: "LSZH", name: "Zurich Airport", city: "Zurich", country: "Switzerland", lat: 47.4647, lon: 8.5492, region: "europe" },
  { iata: "VIE", icao: "LOWW", name: "Vienna International", city: "Vienna", country: "Austria", lat: 48.1103, lon: 16.5697, region: "europe" },
  { iata: "CPH", icao: "EKCH", name: "Copenhagen Airport", city: "Copenhagen", country: "Denmark", lat: 55.618, lon: 12.6508, region: "europe" },
  { iata: "DUB", icao: "EIDW", name: "Dublin Airport", city: "Dublin", country: "Ireland", lat: 53.4264, lon: -6.2499, region: "europe" },
  { iata: "IST", icao: "LTFM", name: "Istanbul Airport", city: "Istanbul", country: "Turkey", lat: 41.2753, lon: 28.7519, region: "europe" },
  { iata: "SVO", icao: "UUEE", name: "Sheremetyevo International", city: "Moscow", country: "Russia", lat: 55.9736, lon: 37.4125, region: "europe" },
  { iata: "ARN", icao: "ESSA", name: "Stockholm Arlanda", city: "Stockholm", country: "Sweden", lat: 59.6519, lon: 17.9186, region: "europe" },
  { iata: "OSL", icao: "ENGM", name: "Oslo Gardermoen", city: "Oslo", country: "Norway", lat: 60.1939, lon: 11.1004, region: "europe" },
  { iata: "HEL", icao: "EFHK", name: "Helsinki-Vantaa", city: "Helsinki", country: "Finland", lat: 60.3172, lon: 24.9633, region: "europe" },
  // Asia-Pacific
  { iata: "HND", icao: "RJTT", name: "Tokyo Haneda", city: "Tokyo", country: "Japan", lat: 35.5494, lon: 139.7798, region: "apac" },
  { iata: "NRT", icao: "RJAA", name: "Narita International", city: "Tokyo", country: "Japan", lat: 35.772, lon: 140.3929, region: "apac" },
  { iata: "PEK", icao: "ZBAA", name: "Beijing Capital", city: "Beijing", country: "China", lat: 40.0799, lon: 116.6031, region: "apac" },
  { iata: "PVG", icao: "ZSPD", name: "Shanghai Pudong", city: "Shanghai", country: "China", lat: 31.1443, lon: 121.8083, region: "apac" },
  { iata: "HKG", icao: "VHHH", name: "Hong Kong International", city: "Hong Kong", country: "China", lat: 22.308, lon: 113.9185, region: "apac" },
  { iata: "SIN", icao: "WSSS", name: "Singapore Changi", city: "Singapore", country: "Singapore", lat: 1.3644, lon: 103.9915, region: "apac" },
  { iata: "ICN", icao: "RKSI", name: "Incheon International", city: "Seoul", country: "South Korea", lat: 37.4602, lon: 126.4407, region: "apac" },
  { iata: "BKK", icao: "VTBS", name: "Suvarnabhumi Airport", city: "Bangkok", country: "Thailand", lat: 13.69, lon: 100.7501, region: "apac" },
  { iata: "SYD", icao: "YSSY", name: "Sydney Kingsford Smith", city: "Sydney", country: "Australia", lat: -33.9461, lon: 151.1772, region: "apac" },
  { iata: "MEL", icao: "YMML", name: "Melbourne Airport", city: "Melbourne", country: "Australia", lat: -37.669, lon: 144.841, region: "apac" },
  { iata: "DEL", icao: "VIDP", name: "Indira Gandhi International", city: "Delhi", country: "India", lat: 28.5562, lon: 77.1, region: "apac" },
  { iata: "BOM", icao: "VABB", name: "Chhatrapati Shivaji Maharaj", city: "Mumbai", country: "India", lat: 19.0896, lon: 72.8656, region: "apac" },
  { iata: "KUL", icao: "WMKK", name: "Kuala Lumpur International", city: "Kuala Lumpur", country: "Malaysia", lat: 2.7456, lon: 101.7099, region: "apac" },
  { iata: "CGK", icao: "WIII", name: "Soekarno-Hatta International", city: "Jakarta", country: "Indonesia", lat: -6.1256, lon: 106.6558, region: "apac" },
  { iata: "MNL", icao: "RPLL", name: "Ninoy Aquino International", city: "Manila", country: "Philippines", lat: 14.5086, lon: 121.0197, region: "apac" },
  { iata: "TPE", icao: "RCTP", name: "Taiwan Taoyuan International", city: "Taipei", country: "Taiwan", lat: 25.0797, lon: 121.2342, region: "apac" },
  { iata: "AKL", icao: "NZAA", name: "Auckland Airport", city: "Auckland", country: "New Zealand", lat: -37.0082, lon: 174.785, region: "apac" },
  // Pakistan
  { iata: "KHI", icao: "OPKC", name: "Jinnah International", city: "Karachi", country: "Pakistan", lat: 24.9065, lon: 67.161, region: "apac" },
  { iata: "ISB", icao: "OPIS", name: "Islamabad International", city: "Islamabad", country: "Pakistan", lat: 33.5605, lon: 72.8526, region: "apac" },
  { iata: "LHE", icao: "OPLA", name: "Allama Iqbal International", city: "Lahore", country: "Pakistan", lat: 31.5216, lon: 74.4036, region: "apac" },
  // Middle East & North Africa
  { iata: "DXB", icao: "OMDB", name: "Dubai International", city: "Dubai", country: "UAE", lat: 25.2532, lon: 55.3657, region: "mena" },
  { iata: "DOH", icao: "OTHH", name: "Hamad International", city: "Doha", country: "Qatar", lat: 25.2731, lon: 51.6081, region: "mena" },
  { iata: "AUH", icao: "OMAA", name: "Abu Dhabi International", city: "Abu Dhabi", country: "UAE", lat: 24.433, lon: 54.6511, region: "mena" },
  { iata: "RUH", icao: "OERK", name: "King Khalid International", city: "Riyadh", country: "Saudi Arabia", lat: 24.9576, lon: 46.6988, region: "mena" },
  { iata: "JED", icao: "OEJN", name: "King Abdulaziz International", city: "Jeddah", country: "Saudi Arabia", lat: 21.6796, lon: 39.1565, region: "mena" },
  { iata: "CAI", icao: "HECA", name: "Cairo International", city: "Cairo", country: "Egypt", lat: 30.1219, lon: 31.4056, region: "mena" },
  { iata: "TLV", icao: "LLBG", name: "Ben Gurion Airport", city: "Tel Aviv", country: "Israel", lat: 32.0055, lon: 34.8854, region: "mena" },
  { iata: "AMM", icao: "OJAI", name: "Queen Alia International", city: "Amman", country: "Jordan", lat: 31.7226, lon: 35.9932, region: "mena" },
  { iata: "BAH", icao: "OBBI", name: "Bahrain International", city: "Manama", country: "Bahrain", lat: 26.2708, lon: 50.6336, region: "mena" },
  { iata: "KWI", icao: "OKBK", name: "Kuwait International", city: "Kuwait City", country: "Kuwait", lat: 29.2266, lon: 47.9689, region: "mena" },
  { iata: "MCT", icao: "OOMS", name: "Muscat International", city: "Muscat", country: "Oman", lat: 23.5933, lon: 58.2844, region: "mena" },
  { iata: "CMN", icao: "GMMN", name: "Mohammed V International", city: "Casablanca", country: "Morocco", lat: 33.3675, lon: -7.5898, region: "mena" },
  { iata: "ALG", icao: "DAAG", name: "Houari Boumediene Airport", city: "Algiers", country: "Algeria", lat: 36.691, lon: 3.2154, region: "mena" },
  { iata: "TUN", icao: "DTTA", name: "Tunis\u2013Carthage International", city: "Tunis", country: "Tunisia", lat: 36.851, lon: 10.2272, region: "mena" },
  // Iran
  { iata: "IKA", icao: "OIIE", name: "Imam Khomeini International", city: "Tehran", country: "Iran", lat: 35.4161, lon: 51.1522, region: "mena" },
  { iata: "THR", icao: "OIII", name: "Mehrabad International", city: "Tehran", country: "Iran", lat: 35.6892, lon: 51.3134, region: "mena" },
  { iata: "MHD", icao: "OIMM", name: "Shahid Hashemi Nejad", city: "Mashhad", country: "Iran", lat: 36.2352, lon: 59.641, region: "mena" },
  { iata: "SYZ", icao: "OISS", name: "Shiraz International", city: "Shiraz", country: "Iran", lat: 29.5392, lon: 52.5899, region: "mena" },
  { iata: "IFN", icao: "OIFM", name: "Isfahan International", city: "Isfahan", country: "Iran", lat: 32.7508, lon: 51.8613, region: "mena" },
  // Iraq
  { iata: "BGW", icao: "ORBI", name: "Baghdad International", city: "Baghdad", country: "Iraq", lat: 33.2625, lon: 44.2346, region: "mena" },
  { iata: "BSR", icao: "ORMM", name: "Basra International", city: "Basra", country: "Iraq", lat: 30.5491, lon: 47.6622, region: "mena" },
  { iata: "EBL", icao: "ORER", name: "Erbil International", city: "Erbil", country: "Iraq", lat: 36.2376, lon: 43.9632, region: "mena" },
  { iata: "NJF", icao: "ORNI", name: "Al Najaf International", city: "Najaf", country: "Iraq", lat: 31.99, lon: 44.404, region: "mena" },
  // Lebanon / Syria / Yemen
  { iata: "BEY", icao: "OLBA", name: "Rafic Hariri International", city: "Beirut", country: "Lebanon", lat: 33.8209, lon: 35.4884, region: "mena" },
  { iata: "DAM", icao: "OSDI", name: "Damascus International", city: "Damascus", country: "Syria", lat: 33.4115, lon: 36.5156, region: "mena" },
  { iata: "ALP", icao: "OSAP", name: "Aleppo International", city: "Aleppo", country: "Syria", lat: 36.1807, lon: 37.2244, region: "mena" },
  { iata: "SAH", icao: "OYSN", name: "Sana'a International", city: "Sana'a", country: "Yemen", lat: 15.4763, lon: 44.2197, region: "mena" },
  { iata: "ADE", icao: "OYAA", name: "Aden International", city: "Aden", country: "Yemen", lat: 12.8295, lon: 45.0288, region: "mena" },
  // UAE / Saudi extras
  { iata: "SHJ", icao: "OMSJ", name: "Sharjah International", city: "Sharjah", country: "UAE", lat: 25.3286, lon: 55.5172, region: "mena" },
  { iata: "DWC", icao: "OMDW", name: "Al Maktoum International", city: "Dubai", country: "UAE", lat: 24.896, lon: 55.1614, region: "mena" },
  { iata: "DMM", icao: "OEDF", name: "King Fahd International", city: "Dammam", country: "Saudi Arabia", lat: 26.4712, lon: 49.7979, region: "mena" },
  { iata: "MED", icao: "OEMA", name: "Prince Mohammad bin Abdulaziz", city: "Medina", country: "Saudi Arabia", lat: 24.5534, lon: 39.7051, region: "mena" },
  // Turkey extras
  { iata: "SAW", icao: "LTFJ", name: "Sabiha G\xF6k\xE7en International", city: "Istanbul", country: "Turkey", lat: 40.8986, lon: 29.3092, region: "mena" },
  { iata: "ESB", icao: "LTAC", name: "Esenbo\u011Fa International", city: "Ankara", country: "Turkey", lat: 40.1281, lon: 32.9951, region: "mena" },
  { iata: "ADB", icao: "LTBJ", name: "Adnan Menderes Airport", city: "Izmir", country: "Turkey", lat: 38.2924, lon: 27.157, region: "mena" },
  { iata: "AYT", icao: "LTAI", name: "Antalya Airport", city: "Antalya", country: "Turkey", lat: 36.8987, lon: 30.8005, region: "mena" },
  // Africa
  { iata: "JNB", icao: "FAOR", name: "O.R. Tambo International", city: "Johannesburg", country: "South Africa", lat: -26.1392, lon: 28.246, region: "africa" },
  { iata: "CPT", icao: "FACT", name: "Cape Town International", city: "Cape Town", country: "South Africa", lat: -33.9715, lon: 18.6021, region: "africa" },
  { iata: "NBO", icao: "HKJK", name: "Jomo Kenyatta International", city: "Nairobi", country: "Kenya", lat: -1.3192, lon: 36.9278, region: "africa" },
  { iata: "LOS", icao: "DNMM", name: "Murtala Muhammed International", city: "Lagos", country: "Nigeria", lat: 6.5774, lon: 3.3212, region: "africa" },
  { iata: "ADD", icao: "HAAB", name: "Bole International", city: "Addis Ababa", country: "Ethiopia", lat: 8.9779, lon: 38.7993, region: "africa" },
  { iata: "ACC", icao: "DGAA", name: "Kotoka International", city: "Accra", country: "Ghana", lat: 5.6052, lon: -0.1668, region: "africa" },
  { iata: "DAR", icao: "HTDA", name: "Julius Nyerere International", city: "Dar es Salaam", country: "Tanzania", lat: -6.8781, lon: 39.2026, region: "africa" },
  { iata: "MRU", icao: "FIMP", name: "Sir Seewoosagur Ramgoolam", city: "Mauritius", country: "Mauritius", lat: -20.4302, lon: 57.6836, region: "africa" },
  // Libya / Sudan
  { iata: "TIP", icao: "HLLT", name: "Mitiga International", city: "Tripoli", country: "Libya", lat: 32.8951, lon: 13.276, region: "africa" },
  { iata: "BEN", icao: "HLLB", name: "Benina International", city: "Benghazi", country: "Libya", lat: 32.0968, lon: 20.2695, region: "africa" },
  { iata: "KRT", icao: "HSSS", name: "Khartoum International", city: "Khartoum", country: "Sudan", lat: 15.5895, lon: 32.5532, region: "africa" }
];
var FAA_AIRPORTS = MONITORED_AIRPORTS.filter(
  (a) => a.country === "USA"
).map((a) => a.iata);
var DELAY_SEVERITY_THRESHOLDS = {
  minor: { avgDelayMinutes: 15, delayedPct: 15 },
  moderate: { avgDelayMinutes: 30, delayedPct: 30 },
  major: { avgDelayMinutes: 45, delayedPct: 45 },
  severe: { avgDelayMinutes: 60, delayedPct: 60 }
};

// node_modules/fast-xml-parser/src/util.js
var nameStartChar = ":A-Za-z_\\u00C0-\\u00D6\\u00D8-\\u00F6\\u00F8-\\u02FF\\u0370-\\u037D\\u037F-\\u1FFF\\u200C-\\u200D\\u2070-\\u218F\\u2C00-\\u2FEF\\u3001-\\uD7FF\\uF900-\\uFDCF\\uFDF0-\\uFFFD";
var nameChar = nameStartChar + "\\-.\\d\\u00B7\\u0300-\\u036F\\u203F-\\u2040";
var nameRegexp = "[" + nameStartChar + "][" + nameChar + "]*";
var regexName = new RegExp("^" + nameRegexp + "$");
function getAllMatches(string, regex) {
  const matches = [];
  let match = regex.exec(string);
  while (match) {
    const allmatches = [];
    allmatches.startIndex = regex.lastIndex - match[0].length;
    const len = match.length;
    for (let index = 0; index < len; index++) {
      allmatches.push(match[index]);
    }
    matches.push(allmatches);
    match = regex.exec(string);
  }
  return matches;
}
var isName = function(string) {
  const match = regexName.exec(string);
  return !(match === null || typeof match === "undefined");
};
function isExist(v) {
  return typeof v !== "undefined";
}

// node_modules/fast-xml-parser/src/validator.js
var defaultOptions = {
  allowBooleanAttributes: false,
  //A tag can have attributes without any value
  unpairedTags: []
};
function validate(xmlData, options) {
  options = Object.assign({}, defaultOptions, options);
  const tags = [];
  let tagFound = false;
  let reachedRoot = false;
  if (xmlData[0] === "\uFEFF") {
    xmlData = xmlData.substr(1);
  }
  for (let i = 0; i < xmlData.length; i++) {
    if (xmlData[i] === "<" && xmlData[i + 1] === "?") {
      i += 2;
      i = readPI(xmlData, i);
      if (i.err) return i;
    } else if (xmlData[i] === "<") {
      let tagStartPos = i;
      i++;
      if (xmlData[i] === "!") {
        i = readCommentAndCDATA(xmlData, i);
        continue;
      } else {
        let closingTag = false;
        if (xmlData[i] === "/") {
          closingTag = true;
          i++;
        }
        let tagName = "";
        for (; i < xmlData.length && xmlData[i] !== ">" && xmlData[i] !== " " && xmlData[i] !== "	" && xmlData[i] !== "\n" && xmlData[i] !== "\r"; i++) {
          tagName += xmlData[i];
        }
        tagName = tagName.trim();
        if (tagName[tagName.length - 1] === "/") {
          tagName = tagName.substring(0, tagName.length - 1);
          i--;
        }
        if (!validateTagName(tagName)) {
          let msg;
          if (tagName.trim().length === 0) {
            msg = "Invalid space after '<'.";
          } else {
            msg = "Tag '" + tagName + "' is an invalid name.";
          }
          return getErrorObject("InvalidTag", msg, getLineNumberForPosition(xmlData, i));
        }
        const result = readAttributeStr(xmlData, i);
        if (result === false) {
          return getErrorObject("InvalidAttr", "Attributes for '" + tagName + "' have open quote.", getLineNumberForPosition(xmlData, i));
        }
        let attrStr = result.value;
        i = result.index;
        if (attrStr[attrStr.length - 1] === "/") {
          const attrStrStart = i - attrStr.length;
          attrStr = attrStr.substring(0, attrStr.length - 1);
          const isValid = validateAttributeString(attrStr, options);
          if (isValid === true) {
            tagFound = true;
          } else {
            return getErrorObject(isValid.err.code, isValid.err.msg, getLineNumberForPosition(xmlData, attrStrStart + isValid.err.line));
          }
        } else if (closingTag) {
          if (!result.tagClosed) {
            return getErrorObject("InvalidTag", "Closing tag '" + tagName + "' doesn't have proper closing.", getLineNumberForPosition(xmlData, i));
          } else if (attrStr.trim().length > 0) {
            return getErrorObject("InvalidTag", "Closing tag '" + tagName + "' can't have attributes or invalid starting.", getLineNumberForPosition(xmlData, tagStartPos));
          } else if (tags.length === 0) {
            return getErrorObject("InvalidTag", "Closing tag '" + tagName + "' has not been opened.", getLineNumberForPosition(xmlData, tagStartPos));
          } else {
            const otg = tags.pop();
            if (tagName !== otg.tagName) {
              let openPos = getLineNumberForPosition(xmlData, otg.tagStartPos);
              return getErrorObject(
                "InvalidTag",
                "Expected closing tag '" + otg.tagName + "' (opened in line " + openPos.line + ", col " + openPos.col + ") instead of closing tag '" + tagName + "'.",
                getLineNumberForPosition(xmlData, tagStartPos)
              );
            }
            if (tags.length == 0) {
              reachedRoot = true;
            }
          }
        } else {
          const isValid = validateAttributeString(attrStr, options);
          if (isValid !== true) {
            return getErrorObject(isValid.err.code, isValid.err.msg, getLineNumberForPosition(xmlData, i - attrStr.length + isValid.err.line));
          }
          if (reachedRoot === true) {
            return getErrorObject("InvalidXml", "Multiple possible root nodes found.", getLineNumberForPosition(xmlData, i));
          } else if (options.unpairedTags.indexOf(tagName) !== -1) {
          } else {
            tags.push({ tagName, tagStartPos });
          }
          tagFound = true;
        }
        for (i++; i < xmlData.length; i++) {
          if (xmlData[i] === "<") {
            if (xmlData[i + 1] === "!") {
              i++;
              i = readCommentAndCDATA(xmlData, i);
              continue;
            } else if (xmlData[i + 1] === "?") {
              i = readPI(xmlData, ++i);
              if (i.err) return i;
            } else {
              break;
            }
          } else if (xmlData[i] === "&") {
            const afterAmp = validateAmpersand(xmlData, i);
            if (afterAmp == -1)
              return getErrorObject("InvalidChar", "char '&' is not expected.", getLineNumberForPosition(xmlData, i));
            i = afterAmp;
          } else {
            if (reachedRoot === true && !isWhiteSpace(xmlData[i])) {
              return getErrorObject("InvalidXml", "Extra text at the end", getLineNumberForPosition(xmlData, i));
            }
          }
        }
        if (xmlData[i] === "<") {
          i--;
        }
      }
    } else {
      if (isWhiteSpace(xmlData[i])) {
        continue;
      }
      return getErrorObject("InvalidChar", "char '" + xmlData[i] + "' is not expected.", getLineNumberForPosition(xmlData, i));
    }
  }
  if (!tagFound) {
    return getErrorObject("InvalidXml", "Start tag expected.", 1);
  } else if (tags.length == 1) {
    return getErrorObject("InvalidTag", "Unclosed tag '" + tags[0].tagName + "'.", getLineNumberForPosition(xmlData, tags[0].tagStartPos));
  } else if (tags.length > 0) {
    return getErrorObject("InvalidXml", "Invalid '" + JSON.stringify(tags.map((t) => t.tagName), null, 4).replace(/\r?\n/g, "") + "' found.", { line: 1, col: 1 });
  }
  return true;
}
function isWhiteSpace(char) {
  return char === " " || char === "	" || char === "\n" || char === "\r";
}
function readPI(xmlData, i) {
  const start = i;
  for (; i < xmlData.length; i++) {
    if (xmlData[i] == "?" || xmlData[i] == " ") {
      const tagname = xmlData.substr(start, i - start);
      if (i > 5 && tagname === "xml") {
        return getErrorObject("InvalidXml", "XML declaration allowed only at the start of the document.", getLineNumberForPosition(xmlData, i));
      } else if (xmlData[i] == "?" && xmlData[i + 1] == ">") {
        i++;
        break;
      } else {
        continue;
      }
    }
  }
  return i;
}
function readCommentAndCDATA(xmlData, i) {
  if (xmlData.length > i + 5 && xmlData[i + 1] === "-" && xmlData[i + 2] === "-") {
    for (i += 3; i < xmlData.length; i++) {
      if (xmlData[i] === "-" && xmlData[i + 1] === "-" && xmlData[i + 2] === ">") {
        i += 2;
        break;
      }
    }
  } else if (xmlData.length > i + 8 && xmlData[i + 1] === "D" && xmlData[i + 2] === "O" && xmlData[i + 3] === "C" && xmlData[i + 4] === "T" && xmlData[i + 5] === "Y" && xmlData[i + 6] === "P" && xmlData[i + 7] === "E") {
    let angleBracketsCount = 1;
    for (i += 8; i < xmlData.length; i++) {
      if (xmlData[i] === "<") {
        angleBracketsCount++;
      } else if (xmlData[i] === ">") {
        angleBracketsCount--;
        if (angleBracketsCount === 0) {
          break;
        }
      }
    }
  } else if (xmlData.length > i + 9 && xmlData[i + 1] === "[" && xmlData[i + 2] === "C" && xmlData[i + 3] === "D" && xmlData[i + 4] === "A" && xmlData[i + 5] === "T" && xmlData[i + 6] === "A" && xmlData[i + 7] === "[") {
    for (i += 8; i < xmlData.length; i++) {
      if (xmlData[i] === "]" && xmlData[i + 1] === "]" && xmlData[i + 2] === ">") {
        i += 2;
        break;
      }
    }
  }
  return i;
}
var doubleQuote = '"';
var singleQuote = "'";
function readAttributeStr(xmlData, i) {
  let attrStr = "";
  let startChar = "";
  let tagClosed = false;
  for (; i < xmlData.length; i++) {
    if (xmlData[i] === doubleQuote || xmlData[i] === singleQuote) {
      if (startChar === "") {
        startChar = xmlData[i];
      } else if (startChar !== xmlData[i]) {
      } else {
        startChar = "";
      }
    } else if (xmlData[i] === ">") {
      if (startChar === "") {
        tagClosed = true;
        break;
      }
    }
    attrStr += xmlData[i];
  }
  if (startChar !== "") {
    return false;
  }
  return {
    value: attrStr,
    index: i,
    tagClosed
  };
}
var validAttrStrRegxp = new RegExp(`(\\s*)([^\\s=]+)(\\s*=)?(\\s*(['"])(([\\s\\S])*?)\\5)?`, "g");
function validateAttributeString(attrStr, options) {
  const matches = getAllMatches(attrStr, validAttrStrRegxp);
  const attrNames = {};
  for (let i = 0; i < matches.length; i++) {
    if (matches[i][1].length === 0) {
      return getErrorObject("InvalidAttr", "Attribute '" + matches[i][2] + "' has no space in starting.", getPositionFromMatch(matches[i]));
    } else if (matches[i][3] !== void 0 && matches[i][4] === void 0) {
      return getErrorObject("InvalidAttr", "Attribute '" + matches[i][2] + "' is without value.", getPositionFromMatch(matches[i]));
    } else if (matches[i][3] === void 0 && !options.allowBooleanAttributes) {
      return getErrorObject("InvalidAttr", "boolean attribute '" + matches[i][2] + "' is not allowed.", getPositionFromMatch(matches[i]));
    }
    const attrName = matches[i][2];
    if (!validateAttrName(attrName)) {
      return getErrorObject("InvalidAttr", "Attribute '" + attrName + "' is an invalid name.", getPositionFromMatch(matches[i]));
    }
    if (!attrNames.hasOwnProperty(attrName)) {
      attrNames[attrName] = 1;
    } else {
      return getErrorObject("InvalidAttr", "Attribute '" + attrName + "' is repeated.", getPositionFromMatch(matches[i]));
    }
  }
  return true;
}
function validateNumberAmpersand(xmlData, i) {
  let re = /\d/;
  if (xmlData[i] === "x") {
    i++;
    re = /[\da-fA-F]/;
  }
  for (; i < xmlData.length; i++) {
    if (xmlData[i] === ";")
      return i;
    if (!xmlData[i].match(re))
      break;
  }
  return -1;
}
function validateAmpersand(xmlData, i) {
  i++;
  if (xmlData[i] === ";")
    return -1;
  if (xmlData[i] === "#") {
    i++;
    return validateNumberAmpersand(xmlData, i);
  }
  let count = 0;
  for (; i < xmlData.length; i++, count++) {
    if (xmlData[i].match(/\w/) && count < 20)
      continue;
    if (xmlData[i] === ";")
      break;
    return -1;
  }
  return i;
}
function getErrorObject(code, message, lineNumber) {
  return {
    err: {
      code,
      msg: message,
      line: lineNumber.line || lineNumber,
      col: lineNumber.col
    }
  };
}
function validateAttrName(attrName) {
  return isName(attrName);
}
function validateTagName(tagname) {
  return isName(tagname);
}
function getLineNumberForPosition(xmlData, index) {
  const lines = xmlData.substring(0, index).split(/\r?\n/);
  return {
    line: lines.length,
    // column number is last line's length + 1, because column numbering starts at 1:
    col: lines[lines.length - 1].length + 1
  };
}
function getPositionFromMatch(match) {
  return match.startIndex + match[1].length;
}

// node_modules/fast-xml-parser/src/xmlparser/OptionsBuilder.js
var defaultOptions2 = {
  preserveOrder: false,
  attributeNamePrefix: "@_",
  attributesGroupName: false,
  textNodeName: "#text",
  ignoreAttributes: true,
  removeNSPrefix: false,
  // remove NS from tag name or attribute name if true
  allowBooleanAttributes: false,
  //a tag can have attributes without any value
  //ignoreRootElement : false,
  parseTagValue: true,
  parseAttributeValue: false,
  trimValues: true,
  //Trim string values of tag and attributes
  cdataPropName: false,
  numberParseOptions: {
    hex: true,
    leadingZeros: true,
    eNotation: true
  },
  tagValueProcessor: function(tagName, val) {
    return val;
  },
  attributeValueProcessor: function(attrName, val) {
    return val;
  },
  stopNodes: [],
  //nested tags will not be parsed even for errors
  alwaysCreateTextNode: false,
  isArray: () => false,
  commentPropName: false,
  unpairedTags: [],
  processEntities: true,
  htmlEntities: false,
  ignoreDeclaration: false,
  ignorePiTags: false,
  transformTagName: false,
  transformAttributeName: false,
  updateTag: function(tagName, jPath, attrs) {
    return tagName;
  },
  // skipEmptyListItem: false
  captureMetaData: false
};
function normalizeProcessEntities(value) {
  if (typeof value === "boolean") {
    return {
      enabled: value,
      // true or false
      maxEntitySize: 1e4,
      maxExpansionDepth: 10,
      maxTotalExpansions: 1e3,
      maxExpandedLength: 1e5,
      allowedTags: null,
      tagFilter: null
    };
  }
  if (typeof value === "object" && value !== null) {
    return {
      enabled: value.enabled !== false,
      // default true if not specified
      maxEntitySize: value.maxEntitySize ?? 1e4,
      maxExpansionDepth: value.maxExpansionDepth ?? 10,
      maxTotalExpansions: value.maxTotalExpansions ?? 1e3,
      maxExpandedLength: value.maxExpandedLength ?? 1e5,
      allowedTags: value.allowedTags ?? null,
      tagFilter: value.tagFilter ?? null
    };
  }
  return normalizeProcessEntities(true);
}
var buildOptions = function(options) {
  const built = Object.assign({}, defaultOptions2, options);
  built.processEntities = normalizeProcessEntities(built.processEntities);
  return built;
};

// node_modules/fast-xml-parser/src/xmlparser/xmlNode.js
var METADATA_SYMBOL;
if (typeof Symbol !== "function") {
  METADATA_SYMBOL = "@@xmlMetadata";
} else {
  METADATA_SYMBOL = /* @__PURE__ */ Symbol("XML Node Metadata");
}
var XmlNode = class {
  constructor(tagname) {
    this.tagname = tagname;
    this.child = [];
    this[":@"] = {};
  }
  add(key, val) {
    if (key === "__proto__") key = "#__proto__";
    this.child.push({ [key]: val });
  }
  addChild(node, startIndex) {
    if (node.tagname === "__proto__") node.tagname = "#__proto__";
    if (node[":@"] && Object.keys(node[":@"]).length > 0) {
      this.child.push({ [node.tagname]: node.child, [":@"]: node[":@"] });
    } else {
      this.child.push({ [node.tagname]: node.child });
    }
    if (startIndex !== void 0) {
      this.child[this.child.length - 1][METADATA_SYMBOL] = { startIndex };
    }
  }
  /** symbol used for metadata */
  static getMetaDataSymbol() {
    return METADATA_SYMBOL;
  }
};

// node_modules/fast-xml-parser/src/xmlparser/DocTypeReader.js
var DocTypeReader = class {
  constructor(options) {
    this.suppressValidationErr = !options;
    this.options = options;
  }
  readDocType(xmlData, i) {
    const entities = {};
    if (xmlData[i + 3] === "O" && xmlData[i + 4] === "C" && xmlData[i + 5] === "T" && xmlData[i + 6] === "Y" && xmlData[i + 7] === "P" && xmlData[i + 8] === "E") {
      i = i + 9;
      let angleBracketsCount = 1;
      let hasBody = false, comment = false;
      let exp = "";
      for (; i < xmlData.length; i++) {
        if (xmlData[i] === "<" && !comment) {
          if (hasBody && hasSeq(xmlData, "!ENTITY", i)) {
            i += 7;
            let entityName, val;
            [entityName, val, i] = this.readEntityExp(xmlData, i + 1, this.suppressValidationErr);
            if (val.indexOf("&") === -1) {
              const escaped = entityName.replace(/[.\-+*:]/g, "\\.");
              entities[entityName] = {
                regx: RegExp(`&${escaped};`, "g"),
                val
              };
            }
          } else if (hasBody && hasSeq(xmlData, "!ELEMENT", i)) {
            i += 8;
            const { index } = this.readElementExp(xmlData, i + 1);
            i = index;
          } else if (hasBody && hasSeq(xmlData, "!ATTLIST", i)) {
            i += 8;
          } else if (hasBody && hasSeq(xmlData, "!NOTATION", i)) {
            i += 9;
            const { index } = this.readNotationExp(xmlData, i + 1, this.suppressValidationErr);
            i = index;
          } else if (hasSeq(xmlData, "!--", i)) comment = true;
          else throw new Error(`Invalid DOCTYPE`);
          angleBracketsCount++;
          exp = "";
        } else if (xmlData[i] === ">") {
          if (comment) {
            if (xmlData[i - 1] === "-" && xmlData[i - 2] === "-") {
              comment = false;
              angleBracketsCount--;
            }
          } else {
            angleBracketsCount--;
          }
          if (angleBracketsCount === 0) {
            break;
          }
        } else if (xmlData[i] === "[") {
          hasBody = true;
        } else {
          exp += xmlData[i];
        }
      }
      if (angleBracketsCount !== 0) {
        throw new Error(`Unclosed DOCTYPE`);
      }
    } else {
      throw new Error(`Invalid Tag instead of DOCTYPE`);
    }
    return { entities, i };
  }
  readEntityExp(xmlData, i) {
    i = skipWhitespace(xmlData, i);
    let entityName = "";
    while (i < xmlData.length && !/\s/.test(xmlData[i]) && xmlData[i] !== '"' && xmlData[i] !== "'") {
      entityName += xmlData[i];
      i++;
    }
    validateEntityName(entityName);
    i = skipWhitespace(xmlData, i);
    if (!this.suppressValidationErr) {
      if (xmlData.substring(i, i + 6).toUpperCase() === "SYSTEM") {
        throw new Error("External entities are not supported");
      } else if (xmlData[i] === "%") {
        throw new Error("Parameter entities are not supported");
      }
    }
    let entityValue = "";
    [i, entityValue] = this.readIdentifierVal(xmlData, i, "entity");
    if (this.options.enabled !== false && this.options.maxEntitySize && entityValue.length > this.options.maxEntitySize) {
      throw new Error(
        `Entity "${entityName}" size (${entityValue.length}) exceeds maximum allowed size (${this.options.maxEntitySize})`
      );
    }
    i--;
    return [entityName, entityValue, i];
  }
  readNotationExp(xmlData, i) {
    i = skipWhitespace(xmlData, i);
    let notationName = "";
    while (i < xmlData.length && !/\s/.test(xmlData[i])) {
      notationName += xmlData[i];
      i++;
    }
    !this.suppressValidationErr && validateEntityName(notationName);
    i = skipWhitespace(xmlData, i);
    const identifierType = xmlData.substring(i, i + 6).toUpperCase();
    if (!this.suppressValidationErr && identifierType !== "SYSTEM" && identifierType !== "PUBLIC") {
      throw new Error(`Expected SYSTEM or PUBLIC, found "${identifierType}"`);
    }
    i += identifierType.length;
    i = skipWhitespace(xmlData, i);
    let publicIdentifier = null;
    let systemIdentifier = null;
    if (identifierType === "PUBLIC") {
      [i, publicIdentifier] = this.readIdentifierVal(xmlData, i, "publicIdentifier");
      i = skipWhitespace(xmlData, i);
      if (xmlData[i] === '"' || xmlData[i] === "'") {
        [i, systemIdentifier] = this.readIdentifierVal(xmlData, i, "systemIdentifier");
      }
    } else if (identifierType === "SYSTEM") {
      [i, systemIdentifier] = this.readIdentifierVal(xmlData, i, "systemIdentifier");
      if (!this.suppressValidationErr && !systemIdentifier) {
        throw new Error("Missing mandatory system identifier for SYSTEM notation");
      }
    }
    return { notationName, publicIdentifier, systemIdentifier, index: --i };
  }
  readIdentifierVal(xmlData, i, type) {
    let identifierVal = "";
    const startChar = xmlData[i];
    if (startChar !== '"' && startChar !== "'") {
      throw new Error(`Expected quoted string, found "${startChar}"`);
    }
    i++;
    while (i < xmlData.length && xmlData[i] !== startChar) {
      identifierVal += xmlData[i];
      i++;
    }
    if (xmlData[i] !== startChar) {
      throw new Error(`Unterminated ${type} value`);
    }
    i++;
    return [i, identifierVal];
  }
  readElementExp(xmlData, i) {
    i = skipWhitespace(xmlData, i);
    let elementName = "";
    while (i < xmlData.length && !/\s/.test(xmlData[i])) {
      elementName += xmlData[i];
      i++;
    }
    if (!this.suppressValidationErr && !isName(elementName)) {
      throw new Error(`Invalid element name: "${elementName}"`);
    }
    i = skipWhitespace(xmlData, i);
    let contentModel = "";
    if (xmlData[i] === "E" && hasSeq(xmlData, "MPTY", i)) i += 4;
    else if (xmlData[i] === "A" && hasSeq(xmlData, "NY", i)) i += 2;
    else if (xmlData[i] === "(") {
      i++;
      while (i < xmlData.length && xmlData[i] !== ")") {
        contentModel += xmlData[i];
        i++;
      }
      if (xmlData[i] !== ")") {
        throw new Error("Unterminated content model");
      }
    } else if (!this.suppressValidationErr) {
      throw new Error(`Invalid Element Expression, found "${xmlData[i]}"`);
    }
    return {
      elementName,
      contentModel: contentModel.trim(),
      index: i
    };
  }
  readAttlistExp(xmlData, i) {
    i = skipWhitespace(xmlData, i);
    let elementName = "";
    while (i < xmlData.length && !/\s/.test(xmlData[i])) {
      elementName += xmlData[i];
      i++;
    }
    validateEntityName(elementName);
    i = skipWhitespace(xmlData, i);
    let attributeName = "";
    while (i < xmlData.length && !/\s/.test(xmlData[i])) {
      attributeName += xmlData[i];
      i++;
    }
    if (!validateEntityName(attributeName)) {
      throw new Error(`Invalid attribute name: "${attributeName}"`);
    }
    i = skipWhitespace(xmlData, i);
    let attributeType = "";
    if (xmlData.substring(i, i + 8).toUpperCase() === "NOTATION") {
      attributeType = "NOTATION";
      i += 8;
      i = skipWhitespace(xmlData, i);
      if (xmlData[i] !== "(") {
        throw new Error(`Expected '(', found "${xmlData[i]}"`);
      }
      i++;
      let allowedNotations = [];
      while (i < xmlData.length && xmlData[i] !== ")") {
        let notation = "";
        while (i < xmlData.length && xmlData[i] !== "|" && xmlData[i] !== ")") {
          notation += xmlData[i];
          i++;
        }
        notation = notation.trim();
        if (!validateEntityName(notation)) {
          throw new Error(`Invalid notation name: "${notation}"`);
        }
        allowedNotations.push(notation);
        if (xmlData[i] === "|") {
          i++;
          i = skipWhitespace(xmlData, i);
        }
      }
      if (xmlData[i] !== ")") {
        throw new Error("Unterminated list of notations");
      }
      i++;
      attributeType += " (" + allowedNotations.join("|") + ")";
    } else {
      while (i < xmlData.length && !/\s/.test(xmlData[i])) {
        attributeType += xmlData[i];
        i++;
      }
      const validTypes = ["CDATA", "ID", "IDREF", "IDREFS", "ENTITY", "ENTITIES", "NMTOKEN", "NMTOKENS"];
      if (!this.suppressValidationErr && !validTypes.includes(attributeType.toUpperCase())) {
        throw new Error(`Invalid attribute type: "${attributeType}"`);
      }
    }
    i = skipWhitespace(xmlData, i);
    let defaultValue = "";
    if (xmlData.substring(i, i + 8).toUpperCase() === "#REQUIRED") {
      defaultValue = "#REQUIRED";
      i += 8;
    } else if (xmlData.substring(i, i + 7).toUpperCase() === "#IMPLIED") {
      defaultValue = "#IMPLIED";
      i += 7;
    } else {
      [i, defaultValue] = this.readIdentifierVal(xmlData, i, "ATTLIST");
    }
    return {
      elementName,
      attributeName,
      attributeType,
      defaultValue,
      index: i
    };
  }
};
var skipWhitespace = (data, index) => {
  while (index < data.length && /\s/.test(data[index])) {
    index++;
  }
  return index;
};
function hasSeq(data, seq, i) {
  for (let j = 0; j < seq.length; j++) {
    if (seq[j] !== data[i + j + 1]) return false;
  }
  return true;
}
function validateEntityName(name) {
  if (isName(name))
    return name;
  else
    throw new Error(`Invalid entity name ${name}`);
}

// node_modules/strnum/strnum.js
var hexRegex = /^[-+]?0x[a-fA-F0-9]+$/;
var numRegex = /^([\-\+])?(0*)([0-9]*(\.[0-9]*)?)$/;
var consider = {
  hex: true,
  // oct: false,
  leadingZeros: true,
  decimalPoint: ".",
  eNotation: true
  //skipLike: /regex/
};
function toNumber(str, options = {}) {
  options = Object.assign({}, consider, options);
  if (!str || typeof str !== "string") return str;
  let trimmedStr = str.trim();
  if (options.skipLike !== void 0 && options.skipLike.test(trimmedStr)) return str;
  else if (str === "0") return 0;
  else if (options.hex && hexRegex.test(trimmedStr)) {
    return parse_int(trimmedStr, 16);
  } else if (trimmedStr.includes("e") || trimmedStr.includes("E")) {
    return resolveEnotation(str, trimmedStr, options);
  } else {
    const match = numRegex.exec(trimmedStr);
    if (match) {
      const sign = match[1] || "";
      const leadingZeros = match[2];
      let numTrimmedByZeros = trimZeros(match[3]);
      const decimalAdjacentToLeadingZeros = sign ? (
        // 0., -00., 000.
        str[leadingZeros.length + 1] === "."
      ) : str[leadingZeros.length] === ".";
      if (!options.leadingZeros && (leadingZeros.length > 1 || leadingZeros.length === 1 && !decimalAdjacentToLeadingZeros)) {
        return str;
      } else {
        const num = Number(trimmedStr);
        const parsedStr = String(num);
        if (num === 0) return num;
        if (parsedStr.search(/[eE]/) !== -1) {
          if (options.eNotation) return num;
          else return str;
        } else if (trimmedStr.indexOf(".") !== -1) {
          if (parsedStr === "0") return num;
          else if (parsedStr === numTrimmedByZeros) return num;
          else if (parsedStr === `${sign}${numTrimmedByZeros}`) return num;
          else return str;
        }
        let n = leadingZeros ? numTrimmedByZeros : trimmedStr;
        if (leadingZeros) {
          return n === parsedStr || sign + n === parsedStr ? num : str;
        } else {
          return n === parsedStr || n === sign + parsedStr ? num : str;
        }
      }
    } else {
      return str;
    }
  }
}
var eNotationRegx = /^([-+])?(0*)(\d*(\.\d*)?[eE][-\+]?\d+)$/;
function resolveEnotation(str, trimmedStr, options) {
  if (!options.eNotation) return str;
  const notation = trimmedStr.match(eNotationRegx);
  if (notation) {
    let sign = notation[1] || "";
    const eChar = notation[3].indexOf("e") === -1 ? "E" : "e";
    const leadingZeros = notation[2];
    const eAdjacentToLeadingZeros = sign ? (
      // 0E.
      str[leadingZeros.length + 1] === eChar
    ) : str[leadingZeros.length] === eChar;
    if (leadingZeros.length > 1 && eAdjacentToLeadingZeros) return str;
    else if (leadingZeros.length === 1 && (notation[3].startsWith(`.${eChar}`) || notation[3][0] === eChar)) {
      return Number(trimmedStr);
    } else if (options.leadingZeros && !eAdjacentToLeadingZeros) {
      trimmedStr = (notation[1] || "") + notation[3];
      return Number(trimmedStr);
    } else return str;
  } else {
    return str;
  }
}
function trimZeros(numStr) {
  if (numStr && numStr.indexOf(".") !== -1) {
    numStr = numStr.replace(/0+$/, "");
    if (numStr === ".") numStr = "0";
    else if (numStr[0] === ".") numStr = "0" + numStr;
    else if (numStr[numStr.length - 1] === ".") numStr = numStr.substring(0, numStr.length - 1);
    return numStr;
  }
  return numStr;
}
function parse_int(numStr, base) {
  if (parseInt) return parseInt(numStr, base);
  else if (Number.parseInt) return Number.parseInt(numStr, base);
  else if (window && window.parseInt) return window.parseInt(numStr, base);
  else throw new Error("parseInt, Number.parseInt, window.parseInt are not supported");
}

// node_modules/fast-xml-parser/src/ignoreAttributes.js
function getIgnoreAttributesFn(ignoreAttributes) {
  if (typeof ignoreAttributes === "function") {
    return ignoreAttributes;
  }
  if (Array.isArray(ignoreAttributes)) {
    return (attrName) => {
      for (const pattern of ignoreAttributes) {
        if (typeof pattern === "string" && attrName === pattern) {
          return true;
        }
        if (pattern instanceof RegExp && pattern.test(attrName)) {
          return true;
        }
      }
    };
  }
  return () => false;
}

// node_modules/fast-xml-parser/src/xmlparser/OrderedObjParser.js
var OrderedObjParser = class {
  constructor(options) {
    this.options = options;
    this.currentNode = null;
    this.tagsNodeStack = [];
    this.docTypeEntities = {};
    this.lastEntities = {
      "apos": { regex: /&(apos|#39|#x27);/g, val: "'" },
      "gt": { regex: /&(gt|#62|#x3E);/g, val: ">" },
      "lt": { regex: /&(lt|#60|#x3C);/g, val: "<" },
      "quot": { regex: /&(quot|#34|#x22);/g, val: '"' }
    };
    this.ampEntity = { regex: /&(amp|#38|#x26);/g, val: "&" };
    this.htmlEntities = {
      "space": { regex: /&(nbsp|#160);/g, val: " " },
      // "lt" : { regex: /&(lt|#60);/g, val: "<" },
      // "gt" : { regex: /&(gt|#62);/g, val: ">" },
      // "amp" : { regex: /&(amp|#38);/g, val: "&" },
      // "quot" : { regex: /&(quot|#34);/g, val: "\"" },
      // "apos" : { regex: /&(apos|#39);/g, val: "'" },
      "cent": { regex: /&(cent|#162);/g, val: "\xA2" },
      "pound": { regex: /&(pound|#163);/g, val: "\xA3" },
      "yen": { regex: /&(yen|#165);/g, val: "\xA5" },
      "euro": { regex: /&(euro|#8364);/g, val: "\u20AC" },
      "copyright": { regex: /&(copy|#169);/g, val: "\xA9" },
      "reg": { regex: /&(reg|#174);/g, val: "\xAE" },
      "inr": { regex: /&(inr|#8377);/g, val: "\u20B9" },
      "num_dec": { regex: /&#([0-9]{1,7});/g, val: (_, str) => fromCodePoint(str, 10, "&#") },
      "num_hex": { regex: /&#x([0-9a-fA-F]{1,6});/g, val: (_, str) => fromCodePoint(str, 16, "&#x") }
    };
    this.addExternalEntities = addExternalEntities;
    this.parseXml = parseXml;
    this.parseTextData = parseTextData;
    this.resolveNameSpace = resolveNameSpace;
    this.buildAttributesMap = buildAttributesMap;
    this.isItStopNode = isItStopNode;
    this.replaceEntitiesValue = replaceEntitiesValue;
    this.readStopNodeData = readStopNodeData;
    this.saveTextToParentTag = saveTextToParentTag;
    this.addChild = addChild;
    this.ignoreAttributesFn = getIgnoreAttributesFn(this.options.ignoreAttributes);
    this.entityExpansionCount = 0;
    this.currentExpandedLength = 0;
    if (this.options.stopNodes && this.options.stopNodes.length > 0) {
      this.stopNodesExact = /* @__PURE__ */ new Set();
      this.stopNodesWildcard = /* @__PURE__ */ new Set();
      for (let i = 0; i < this.options.stopNodes.length; i++) {
        const stopNodeExp = this.options.stopNodes[i];
        if (typeof stopNodeExp !== "string") continue;
        if (stopNodeExp.startsWith("*.")) {
          this.stopNodesWildcard.add(stopNodeExp.substring(2));
        } else {
          this.stopNodesExact.add(stopNodeExp);
        }
      }
    }
  }
};
function addExternalEntities(externalEntities) {
  const entKeys = Object.keys(externalEntities);
  for (let i = 0; i < entKeys.length; i++) {
    const ent = entKeys[i];
    const escaped = ent.replace(/[.\-+*:]/g, "\\.");
    this.lastEntities[ent] = {
      regex: new RegExp("&" + escaped + ";", "g"),
      val: externalEntities[ent]
    };
  }
}
function parseTextData(val, tagName, jPath, dontTrim, hasAttributes, isLeafNode, escapeEntities) {
  if (val !== void 0) {
    if (this.options.trimValues && !dontTrim) {
      val = val.trim();
    }
    if (val.length > 0) {
      if (!escapeEntities) val = this.replaceEntitiesValue(val, tagName, jPath);
      const newval = this.options.tagValueProcessor(tagName, val, jPath, hasAttributes, isLeafNode);
      if (newval === null || newval === void 0) {
        return val;
      } else if (typeof newval !== typeof val || newval !== val) {
        return newval;
      } else if (this.options.trimValues) {
        return parseValue(val, this.options.parseTagValue, this.options.numberParseOptions);
      } else {
        const trimmedVal = val.trim();
        if (trimmedVal === val) {
          return parseValue(val, this.options.parseTagValue, this.options.numberParseOptions);
        } else {
          return val;
        }
      }
    }
  }
}
function resolveNameSpace(tagname) {
  if (this.options.removeNSPrefix) {
    const tags = tagname.split(":");
    const prefix = tagname.charAt(0) === "/" ? "/" : "";
    if (tags[0] === "xmlns") {
      return "";
    }
    if (tags.length === 2) {
      tagname = prefix + tags[1];
    }
  }
  return tagname;
}
var attrsRegx = new RegExp(`([^\\s=]+)\\s*(=\\s*(['"])([\\s\\S]*?)\\3)?`, "gm");
function buildAttributesMap(attrStr, jPath, tagName) {
  if (this.options.ignoreAttributes !== true && typeof attrStr === "string") {
    const matches = getAllMatches(attrStr, attrsRegx);
    const len = matches.length;
    const attrs = {};
    for (let i = 0; i < len; i++) {
      const attrName = this.resolveNameSpace(matches[i][1]);
      if (this.ignoreAttributesFn(attrName, jPath)) {
        continue;
      }
      let oldVal = matches[i][4];
      let aName = this.options.attributeNamePrefix + attrName;
      if (attrName.length) {
        if (this.options.transformAttributeName) {
          aName = this.options.transformAttributeName(aName);
        }
        if (aName === "__proto__") aName = "#__proto__";
        if (oldVal !== void 0) {
          if (this.options.trimValues) {
            oldVal = oldVal.trim();
          }
          oldVal = this.replaceEntitiesValue(oldVal, tagName, jPath);
          const newVal = this.options.attributeValueProcessor(attrName, oldVal, jPath);
          if (newVal === null || newVal === void 0) {
            attrs[aName] = oldVal;
          } else if (typeof newVal !== typeof oldVal || newVal !== oldVal) {
            attrs[aName] = newVal;
          } else {
            attrs[aName] = parseValue(
              oldVal,
              this.options.parseAttributeValue,
              this.options.numberParseOptions
            );
          }
        } else if (this.options.allowBooleanAttributes) {
          attrs[aName] = true;
        }
      }
    }
    if (!Object.keys(attrs).length) {
      return;
    }
    if (this.options.attributesGroupName) {
      const attrCollection = {};
      attrCollection[this.options.attributesGroupName] = attrs;
      return attrCollection;
    }
    return attrs;
  }
}
var parseXml = function(xmlData) {
  xmlData = xmlData.replace(/\r\n?/g, "\n");
  const xmlObj = new XmlNode("!xml");
  let currentNode = xmlObj;
  let textData = "";
  let jPath = "";
  this.entityExpansionCount = 0;
  this.currentExpandedLength = 0;
  const docTypeReader = new DocTypeReader(this.options.processEntities);
  for (let i = 0; i < xmlData.length; i++) {
    const ch = xmlData[i];
    if (ch === "<") {
      if (xmlData[i + 1] === "/") {
        const closeIndex = findClosingIndex(xmlData, ">", i, "Closing Tag is not closed.");
        let tagName = xmlData.substring(i + 2, closeIndex).trim();
        if (this.options.removeNSPrefix) {
          const colonIndex = tagName.indexOf(":");
          if (colonIndex !== -1) {
            tagName = tagName.substr(colonIndex + 1);
          }
        }
        if (this.options.transformTagName) {
          tagName = this.options.transformTagName(tagName);
        }
        if (currentNode) {
          textData = this.saveTextToParentTag(textData, currentNode, jPath);
        }
        const lastTagName = jPath.substring(jPath.lastIndexOf(".") + 1);
        if (tagName && this.options.unpairedTags.indexOf(tagName) !== -1) {
          throw new Error(`Unpaired tag can not be used as closing tag: </${tagName}>`);
        }
        let propIndex = 0;
        if (lastTagName && this.options.unpairedTags.indexOf(lastTagName) !== -1) {
          propIndex = jPath.lastIndexOf(".", jPath.lastIndexOf(".") - 1);
          this.tagsNodeStack.pop();
        } else {
          propIndex = jPath.lastIndexOf(".");
        }
        jPath = jPath.substring(0, propIndex);
        currentNode = this.tagsNodeStack.pop();
        textData = "";
        i = closeIndex;
      } else if (xmlData[i + 1] === "?") {
        let tagData = readTagExp(xmlData, i, false, "?>");
        if (!tagData) throw new Error("Pi Tag is not closed.");
        textData = this.saveTextToParentTag(textData, currentNode, jPath);
        if (this.options.ignoreDeclaration && tagData.tagName === "?xml" || this.options.ignorePiTags) {
        } else {
          const childNode = new XmlNode(tagData.tagName);
          childNode.add(this.options.textNodeName, "");
          if (tagData.tagName !== tagData.tagExp && tagData.attrExpPresent) {
            childNode[":@"] = this.buildAttributesMap(tagData.tagExp, jPath, tagData.tagName);
          }
          this.addChild(currentNode, childNode, jPath, i);
        }
        i = tagData.closeIndex + 1;
      } else if (xmlData.substr(i + 1, 3) === "!--") {
        const endIndex = findClosingIndex(xmlData, "-->", i + 4, "Comment is not closed.");
        if (this.options.commentPropName) {
          const comment = xmlData.substring(i + 4, endIndex - 2);
          textData = this.saveTextToParentTag(textData, currentNode, jPath);
          currentNode.add(this.options.commentPropName, [{ [this.options.textNodeName]: comment }]);
        }
        i = endIndex;
      } else if (xmlData.substr(i + 1, 2) === "!D") {
        const result = docTypeReader.readDocType(xmlData, i);
        this.docTypeEntities = result.entities;
        i = result.i;
      } else if (xmlData.substr(i + 1, 2) === "![") {
        const closeIndex = findClosingIndex(xmlData, "]]>", i, "CDATA is not closed.") - 2;
        const tagExp = xmlData.substring(i + 9, closeIndex);
        textData = this.saveTextToParentTag(textData, currentNode, jPath);
        let val = this.parseTextData(tagExp, currentNode.tagname, jPath, true, false, true, true);
        if (val == void 0) val = "";
        if (this.options.cdataPropName) {
          currentNode.add(this.options.cdataPropName, [{ [this.options.textNodeName]: tagExp }]);
        } else {
          currentNode.add(this.options.textNodeName, val);
        }
        i = closeIndex + 2;
      } else {
        let result = readTagExp(xmlData, i, this.options.removeNSPrefix);
        let tagName = result.tagName;
        const rawTagName = result.rawTagName;
        let tagExp = result.tagExp;
        let attrExpPresent = result.attrExpPresent;
        let closeIndex = result.closeIndex;
        if (this.options.transformTagName) {
          const newTagName = this.options.transformTagName(tagName);
          if (tagExp === tagName) {
            tagExp = newTagName;
          }
          tagName = newTagName;
        }
        if (currentNode && textData) {
          if (currentNode.tagname !== "!xml") {
            textData = this.saveTextToParentTag(textData, currentNode, jPath, false);
          }
        }
        const lastTag = currentNode;
        if (lastTag && this.options.unpairedTags.indexOf(lastTag.tagname) !== -1) {
          currentNode = this.tagsNodeStack.pop();
          jPath = jPath.substring(0, jPath.lastIndexOf("."));
        }
        if (tagName !== xmlObj.tagname) {
          jPath += jPath ? "." + tagName : tagName;
        }
        const startIndex = i;
        if (this.isItStopNode(this.stopNodesExact, this.stopNodesWildcard, jPath, tagName)) {
          let tagContent = "";
          if (tagExp.length > 0 && tagExp.lastIndexOf("/") === tagExp.length - 1) {
            if (tagName[tagName.length - 1] === "/") {
              tagName = tagName.substr(0, tagName.length - 1);
              jPath = jPath.substr(0, jPath.length - 1);
              tagExp = tagName;
            } else {
              tagExp = tagExp.substr(0, tagExp.length - 1);
            }
            i = result.closeIndex;
          } else if (this.options.unpairedTags.indexOf(tagName) !== -1) {
            i = result.closeIndex;
          } else {
            const result2 = this.readStopNodeData(xmlData, rawTagName, closeIndex + 1);
            if (!result2) throw new Error(`Unexpected end of ${rawTagName}`);
            i = result2.i;
            tagContent = result2.tagContent;
          }
          const childNode = new XmlNode(tagName);
          if (tagName !== tagExp && attrExpPresent) {
            childNode[":@"] = this.buildAttributesMap(tagExp, jPath, tagName);
          }
          if (tagContent) {
            tagContent = this.parseTextData(tagContent, tagName, jPath, true, attrExpPresent, true, true);
          }
          jPath = jPath.substr(0, jPath.lastIndexOf("."));
          childNode.add(this.options.textNodeName, tagContent);
          this.addChild(currentNode, childNode, jPath, startIndex);
        } else {
          if (tagExp.length > 0 && tagExp.lastIndexOf("/") === tagExp.length - 1) {
            if (tagName[tagName.length - 1] === "/") {
              tagName = tagName.substr(0, tagName.length - 1);
              jPath = jPath.substr(0, jPath.length - 1);
              tagExp = tagName;
            } else {
              tagExp = tagExp.substr(0, tagExp.length - 1);
            }
            if (this.options.transformTagName) {
              const newTagName = this.options.transformTagName(tagName);
              if (tagExp === tagName) {
                tagExp = newTagName;
              }
              tagName = newTagName;
            }
            const childNode = new XmlNode(tagName);
            if (tagName !== tagExp && attrExpPresent) {
              childNode[":@"] = this.buildAttributesMap(tagExp, jPath, tagName);
            }
            this.addChild(currentNode, childNode, jPath, startIndex);
            jPath = jPath.substr(0, jPath.lastIndexOf("."));
          } else {
            const childNode = new XmlNode(tagName);
            this.tagsNodeStack.push(currentNode);
            if (tagName !== tagExp && attrExpPresent) {
              childNode[":@"] = this.buildAttributesMap(tagExp, jPath, tagName);
            }
            this.addChild(currentNode, childNode, jPath, startIndex);
            currentNode = childNode;
          }
          textData = "";
          i = closeIndex;
        }
      }
    } else {
      textData += xmlData[i];
    }
  }
  return xmlObj.child;
};
function addChild(currentNode, childNode, jPath, startIndex) {
  if (!this.options.captureMetaData) startIndex = void 0;
  const result = this.options.updateTag(childNode.tagname, jPath, childNode[":@"]);
  if (result === false) {
  } else if (typeof result === "string") {
    childNode.tagname = result;
    currentNode.addChild(childNode, startIndex);
  } else {
    currentNode.addChild(childNode, startIndex);
  }
}
var replaceEntitiesValue = function(val, tagName, jPath) {
  if (val.indexOf("&") === -1) {
    return val;
  }
  const entityConfig = this.options.processEntities;
  if (!entityConfig.enabled) {
    return val;
  }
  if (entityConfig.allowedTags) {
    if (!entityConfig.allowedTags.includes(tagName)) {
      return val;
    }
  }
  if (entityConfig.tagFilter) {
    if (!entityConfig.tagFilter(tagName, jPath)) {
      return val;
    }
  }
  for (let entityName in this.docTypeEntities) {
    const entity = this.docTypeEntities[entityName];
    const matches = val.match(entity.regx);
    if (matches) {
      this.entityExpansionCount += matches.length;
      if (entityConfig.maxTotalExpansions && this.entityExpansionCount > entityConfig.maxTotalExpansions) {
        throw new Error(
          `Entity expansion limit exceeded: ${this.entityExpansionCount} > ${entityConfig.maxTotalExpansions}`
        );
      }
      const lengthBefore = val.length;
      val = val.replace(entity.regx, entity.val);
      if (entityConfig.maxExpandedLength) {
        this.currentExpandedLength += val.length - lengthBefore;
        if (this.currentExpandedLength > entityConfig.maxExpandedLength) {
          throw new Error(
            `Total expanded content size exceeded: ${this.currentExpandedLength} > ${entityConfig.maxExpandedLength}`
          );
        }
      }
    }
  }
  if (val.indexOf("&") === -1) return val;
  for (let entityName in this.lastEntities) {
    const entity = this.lastEntities[entityName];
    val = val.replace(entity.regex, entity.val);
  }
  if (val.indexOf("&") === -1) return val;
  if (this.options.htmlEntities) {
    for (let entityName in this.htmlEntities) {
      const entity = this.htmlEntities[entityName];
      val = val.replace(entity.regex, entity.val);
    }
  }
  val = val.replace(this.ampEntity.regex, this.ampEntity.val);
  return val;
};
function saveTextToParentTag(textData, currentNode, jPath, isLeafNode) {
  if (textData) {
    if (isLeafNode === void 0) isLeafNode = currentNode.child.length === 0;
    textData = this.parseTextData(
      textData,
      currentNode.tagname,
      jPath,
      false,
      currentNode[":@"] ? Object.keys(currentNode[":@"]).length !== 0 : false,
      isLeafNode
    );
    if (textData !== void 0 && textData !== "")
      currentNode.add(this.options.textNodeName, textData);
    textData = "";
  }
  return textData;
}
function isItStopNode(stopNodesExact, stopNodesWildcard, jPath, currentTagName) {
  if (stopNodesWildcard && stopNodesWildcard.has(currentTagName)) return true;
  if (stopNodesExact && stopNodesExact.has(jPath)) return true;
  return false;
}
function tagExpWithClosingIndex(xmlData, i, closingChar = ">") {
  let attrBoundary;
  let tagExp = "";
  for (let index = i; index < xmlData.length; index++) {
    let ch = xmlData[index];
    if (attrBoundary) {
      if (ch === attrBoundary) attrBoundary = "";
    } else if (ch === '"' || ch === "'") {
      attrBoundary = ch;
    } else if (ch === closingChar[0]) {
      if (closingChar[1]) {
        if (xmlData[index + 1] === closingChar[1]) {
          return {
            data: tagExp,
            index
          };
        }
      } else {
        return {
          data: tagExp,
          index
        };
      }
    } else if (ch === "	") {
      ch = " ";
    }
    tagExp += ch;
  }
}
function findClosingIndex(xmlData, str, i, errMsg) {
  const closingIndex = xmlData.indexOf(str, i);
  if (closingIndex === -1) {
    throw new Error(errMsg);
  } else {
    return closingIndex + str.length - 1;
  }
}
function readTagExp(xmlData, i, removeNSPrefix, closingChar = ">") {
  const result = tagExpWithClosingIndex(xmlData, i + 1, closingChar);
  if (!result) return;
  let tagExp = result.data;
  const closeIndex = result.index;
  const separatorIndex = tagExp.search(/\s/);
  let tagName = tagExp;
  let attrExpPresent = true;
  if (separatorIndex !== -1) {
    tagName = tagExp.substring(0, separatorIndex);
    tagExp = tagExp.substring(separatorIndex + 1).trimStart();
  }
  const rawTagName = tagName;
  if (removeNSPrefix) {
    const colonIndex = tagName.indexOf(":");
    if (colonIndex !== -1) {
      tagName = tagName.substr(colonIndex + 1);
      attrExpPresent = tagName !== result.data.substr(colonIndex + 1);
    }
  }
  return {
    tagName,
    tagExp,
    closeIndex,
    attrExpPresent,
    rawTagName
  };
}
function readStopNodeData(xmlData, tagName, i) {
  const startIndex = i;
  let openTagCount = 1;
  for (; i < xmlData.length; i++) {
    if (xmlData[i] === "<") {
      if (xmlData[i + 1] === "/") {
        const closeIndex = findClosingIndex(xmlData, ">", i, `${tagName} is not closed`);
        let closeTagName = xmlData.substring(i + 2, closeIndex).trim();
        if (closeTagName === tagName) {
          openTagCount--;
          if (openTagCount === 0) {
            return {
              tagContent: xmlData.substring(startIndex, i),
              i: closeIndex
            };
          }
        }
        i = closeIndex;
      } else if (xmlData[i + 1] === "?") {
        const closeIndex = findClosingIndex(xmlData, "?>", i + 1, "StopNode is not closed.");
        i = closeIndex;
      } else if (xmlData.substr(i + 1, 3) === "!--") {
        const closeIndex = findClosingIndex(xmlData, "-->", i + 3, "StopNode is not closed.");
        i = closeIndex;
      } else if (xmlData.substr(i + 1, 2) === "![") {
        const closeIndex = findClosingIndex(xmlData, "]]>", i, "StopNode is not closed.") - 2;
        i = closeIndex;
      } else {
        const tagData = readTagExp(xmlData, i, ">");
        if (tagData) {
          const openTagName = tagData && tagData.tagName;
          if (openTagName === tagName && tagData.tagExp[tagData.tagExp.length - 1] !== "/") {
            openTagCount++;
          }
          i = tagData.closeIndex;
        }
      }
    }
  }
}
function parseValue(val, shouldParse, options) {
  if (shouldParse && typeof val === "string") {
    const newval = val.trim();
    if (newval === "true") return true;
    else if (newval === "false") return false;
    else return toNumber(val, options);
  } else {
    if (isExist(val)) {
      return val;
    } else {
      return "";
    }
  }
}
function fromCodePoint(str, base, prefix) {
  const codePoint = Number.parseInt(str, base);
  if (codePoint >= 0 && codePoint <= 1114111) {
    return String.fromCodePoint(codePoint);
  } else {
    return prefix + str + ";";
  }
}

// node_modules/fast-xml-parser/src/xmlparser/node2json.js
var METADATA_SYMBOL2 = XmlNode.getMetaDataSymbol();
function prettify(node, options) {
  return compress(node, options);
}
function compress(arr, options, jPath) {
  let text;
  const compressedObj = {};
  for (let i = 0; i < arr.length; i++) {
    const tagObj = arr[i];
    const property = propName(tagObj);
    let newJpath = "";
    if (jPath === void 0) newJpath = property;
    else newJpath = jPath + "." + property;
    if (property === options.textNodeName) {
      if (text === void 0) text = tagObj[property];
      else text += "" + tagObj[property];
    } else if (property === void 0) {
      continue;
    } else if (tagObj[property]) {
      let val = compress(tagObj[property], options, newJpath);
      const isLeaf = isLeafTag(val, options);
      if (tagObj[METADATA_SYMBOL2] !== void 0) {
        val[METADATA_SYMBOL2] = tagObj[METADATA_SYMBOL2];
      }
      if (tagObj[":@"]) {
        assignAttributes(val, tagObj[":@"], newJpath, options);
      } else if (Object.keys(val).length === 1 && val[options.textNodeName] !== void 0 && !options.alwaysCreateTextNode) {
        val = val[options.textNodeName];
      } else if (Object.keys(val).length === 0) {
        if (options.alwaysCreateTextNode) val[options.textNodeName] = "";
        else val = "";
      }
      if (compressedObj[property] !== void 0 && compressedObj.hasOwnProperty(property)) {
        if (!Array.isArray(compressedObj[property])) {
          compressedObj[property] = [compressedObj[property]];
        }
        compressedObj[property].push(val);
      } else {
        if (options.isArray(property, newJpath, isLeaf)) {
          compressedObj[property] = [val];
        } else {
          compressedObj[property] = val;
        }
      }
    }
  }
  if (typeof text === "string") {
    if (text.length > 0) compressedObj[options.textNodeName] = text;
  } else if (text !== void 0) compressedObj[options.textNodeName] = text;
  return compressedObj;
}
function propName(obj) {
  const keys = Object.keys(obj);
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    if (key !== ":@") return key;
  }
}
function assignAttributes(obj, attrMap, jpath, options) {
  if (attrMap) {
    const keys = Object.keys(attrMap);
    const len = keys.length;
    for (let i = 0; i < len; i++) {
      const atrrName = keys[i];
      if (options.isArray(atrrName, jpath + "." + atrrName, true, true)) {
        obj[atrrName] = [attrMap[atrrName]];
      } else {
        obj[atrrName] = attrMap[atrrName];
      }
    }
  }
}
function isLeafTag(obj, options) {
  const { textNodeName } = options;
  const propCount = Object.keys(obj).length;
  if (propCount === 0) {
    return true;
  }
  if (propCount === 1 && (obj[textNodeName] || typeof obj[textNodeName] === "boolean" || obj[textNodeName] === 0)) {
    return true;
  }
  return false;
}

// node_modules/fast-xml-parser/src/xmlparser/XMLParser.js
var XMLParser = class {
  constructor(options) {
    this.externalEntities = {};
    this.options = buildOptions(options);
  }
  /**
   * Parse XML dats to JS object 
   * @param {string|Uint8Array} xmlData 
   * @param {boolean|Object} validationOption 
   */
  parse(xmlData, validationOption) {
    if (typeof xmlData !== "string" && xmlData.toString) {
      xmlData = xmlData.toString();
    } else if (typeof xmlData !== "string") {
      throw new Error("XML data is accepted in String or Bytes[] form.");
    }
    if (validationOption) {
      if (validationOption === true) validationOption = {};
      const result = validate(xmlData, validationOption);
      if (result !== true) {
        throw Error(`${result.err.msg}:${result.err.line}:${result.err.col}`);
      }
    }
    const orderedObjParser = new OrderedObjParser(this.options);
    orderedObjParser.addExternalEntities(this.externalEntities);
    const orderedResult = orderedObjParser.parseXml(xmlData);
    if (this.options.preserveOrder || orderedResult === void 0) return orderedResult;
    else return prettify(orderedResult, this.options);
  }
  /**
   * Add Entity which is not by default supported by this library
   * @param {string} key 
   * @param {string} value 
   */
  addEntity(key, value) {
    if (value.indexOf("&") !== -1) {
      throw new Error("Entity value can't have '&'");
    } else if (key.indexOf("&") !== -1 || key.indexOf(";") !== -1) {
      throw new Error("An entity must be set without '&' and ';'. Eg. use '#xD' for '&#xD;'");
    } else if (value === "&") {
      throw new Error("An entity with value '&' is not permitted");
    } else {
      this.externalEntities[key] = value;
    }
  }
  /**
   * Returns a Symbol that can be used to access the metadata
   * property on a node.
   * 
   * If Symbol is not available in the environment, an ordinary property is used
   * and the name of the property is here returned.
   * 
   * The XMLMetaData property is only present when `captureMetaData`
   * is true in the options.
   */
  static getMetaDataSymbol() {
    return XmlNode.getMetaDataSymbol();
  }
};

// server/worldmonitor/aviation/v1/_shared.ts
var FAA_URL = "https://nasstatus.faa.gov/api/airport-status-information";
var AVIATIONSTACK_URL = "https://api.aviationstack.com/v1/flights";
var ICAO_NOTAM_URL = "https://dataservices.icao.int/api/notams-realtime-list";
var BATCH_CONCURRENCY = 10;
var MIN_FLIGHTS_FOR_CLOSURE = 10;
var NOTAM_CLOSURE_QCODES = /* @__PURE__ */ new Set(["FA", "AH", "AL", "AW", "AC", "AM"]);
var xmlParser = new XMLParser({
  ignoreAttributes: true,
  isArray: (_name, jpath) => {
    return /\.(Ground_Delay|Ground_Stop|Delay|Airport)$/.test(jpath);
  }
});
function parseDelayTypeFromReason(reason) {
  const r = reason.toLowerCase();
  if (r.includes("ground stop")) return "ground_stop";
  if (r.includes("ground delay") || r.includes("gdp")) return "ground_delay";
  if (r.includes("departure")) return "departure_delay";
  if (r.includes("arrival")) return "arrival_delay";
  if (r.includes("clos")) return "ground_stop";
  return "general";
}
function parseFaaXml(xml) {
  const delays = /* @__PURE__ */ new Map();
  const parsed = xmlParser.parse(xml);
  const root = parsed?.AIRPORT_STATUS_INFORMATION;
  if (!root) return delays;
  const delayTypes = Array.isArray(root.Delay_type) ? root.Delay_type : root.Delay_type ? [root.Delay_type] : [];
  for (const dt of delayTypes) {
    if (dt.Ground_Delay_List?.Ground_Delay) {
      for (const gd of dt.Ground_Delay_List.Ground_Delay) {
        if (gd.ARPT) {
          delays.set(gd.ARPT, {
            airport: gd.ARPT,
            reason: gd.Reason || "Ground delay",
            avgDelay: gd.Avg ? parseInt(gd.Avg, 10) : 30,
            type: "ground_delay"
          });
        }
      }
    }
    if (dt.Ground_Stop_List?.Ground_Stop) {
      for (const gs of dt.Ground_Stop_List.Ground_Stop) {
        if (gs.ARPT) {
          delays.set(gs.ARPT, {
            airport: gs.ARPT,
            reason: gs.Reason || "Ground stop",
            avgDelay: 60,
            type: "ground_stop"
          });
        }
      }
    }
    if (dt.Arrival_Departure_Delay_List?.Delay) {
      for (const d of dt.Arrival_Departure_Delay_List.Delay) {
        if (d.ARPT) {
          const min = parseInt(d.Arrival_Delay?.Min || d.Departure_Delay?.Min || "15", 10);
          const max = parseInt(d.Arrival_Delay?.Max || d.Departure_Delay?.Max || "30", 10);
          const existing = delays.get(d.ARPT);
          if (!existing || existing.type !== "ground_stop") {
            delays.set(d.ARPT, {
              airport: d.ARPT,
              reason: d.Reason || "Delays",
              avgDelay: Math.round((min + max) / 2),
              type: parseDelayTypeFromReason(d.Reason || "")
            });
          }
        }
      }
    }
    if (dt.Airport_Closure_List?.Airport) {
      for (const ac of dt.Airport_Closure_List.Airport) {
        if (ac.ARPT && FAA_AIRPORTS.includes(ac.ARPT)) {
          delays.set(ac.ARPT, {
            airport: ac.ARPT,
            reason: "Airport closure",
            avgDelay: 120,
            type: "ground_stop"
          });
        }
      }
    }
  }
  return delays;
}
function toProtoDelayType(t) {
  const map = {
    ground_stop: "FLIGHT_DELAY_TYPE_GROUND_STOP",
    ground_delay: "FLIGHT_DELAY_TYPE_GROUND_DELAY",
    departure_delay: "FLIGHT_DELAY_TYPE_DEPARTURE_DELAY",
    arrival_delay: "FLIGHT_DELAY_TYPE_ARRIVAL_DELAY",
    general: "FLIGHT_DELAY_TYPE_GENERAL",
    closure: "FLIGHT_DELAY_TYPE_CLOSURE"
  };
  return map[t] || "FLIGHT_DELAY_TYPE_GENERAL";
}
function toProtoSeverity(s) {
  const map = {
    normal: "FLIGHT_DELAY_SEVERITY_NORMAL",
    minor: "FLIGHT_DELAY_SEVERITY_MINOR",
    moderate: "FLIGHT_DELAY_SEVERITY_MODERATE",
    major: "FLIGHT_DELAY_SEVERITY_MAJOR",
    severe: "FLIGHT_DELAY_SEVERITY_SEVERE"
  };
  return map[s] || "FLIGHT_DELAY_SEVERITY_NORMAL";
}
function toProtoRegion(r) {
  const map = {
    americas: "AIRPORT_REGION_AMERICAS",
    europe: "AIRPORT_REGION_EUROPE",
    apac: "AIRPORT_REGION_APAC",
    mena: "AIRPORT_REGION_MENA",
    africa: "AIRPORT_REGION_AFRICA"
  };
  return map[r] || "AIRPORT_REGION_UNSPECIFIED";
}
function toProtoSource(s) {
  const map = {
    faa: "FLIGHT_DELAY_SOURCE_FAA",
    eurocontrol: "FLIGHT_DELAY_SOURCE_EUROCONTROL",
    computed: "FLIGHT_DELAY_SOURCE_COMPUTED"
  };
  return map[s] || "FLIGHT_DELAY_SOURCE_COMPUTED";
}
function determineSeverity(avgDelayMinutes, delayedPct) {
  const t = DELAY_SEVERITY_THRESHOLDS;
  if (avgDelayMinutes >= t.severe.avgDelayMinutes || delayedPct && delayedPct >= t.severe.delayedPct) return "severe";
  if (avgDelayMinutes >= t.major.avgDelayMinutes || delayedPct && delayedPct >= t.major.delayedPct) return "major";
  if (avgDelayMinutes >= t.moderate.avgDelayMinutes || delayedPct && delayedPct >= t.moderate.delayedPct) return "moderate";
  if (avgDelayMinutes >= t.minor.avgDelayMinutes || delayedPct && delayedPct >= t.minor.delayedPct) return "minor";
  return "normal";
}
async function fetchAviationStackDelays(allAirports) {
  const apiKey = process.env.AVIATIONSTACK_API;
  if (!apiKey) {
    console.log("[Aviation] No AVIATIONSTACK_API key \u2014 skipping");
    return { alerts: [], healthy: false };
  }
  console.log(`[Aviation] Querying ${allAirports.length} airports (concurrency=${BATCH_CONCURRENCY})`);
  const alerts = [];
  let succeeded = 0, failed = 0;
  const deadline = Date.now() + 5e4;
  for (let i = 0; i < allAirports.length; i += BATCH_CONCURRENCY) {
    if (Date.now() >= deadline) {
      console.warn(`[Aviation] Deadline hit after ${succeeded + failed}/${allAirports.length} airports`);
      break;
    }
    const chunk = allAirports.slice(i, i + BATCH_CONCURRENCY);
    const results = await Promise.allSettled(
      chunk.map((airport) => fetchSingleAirport(apiKey, airport))
    );
    for (const r of results) {
      if (r.status === "fulfilled") {
        if (r.value.ok) {
          succeeded++;
          if (r.value.alert) alerts.push(r.value.alert);
        } else failed++;
      } else {
        failed++;
      }
    }
  }
  const healthy = allAirports.length < 5 || failed <= succeeded;
  console.log(`[Aviation] Done: ${succeeded} ok, ${failed} failed, ${alerts.length} alerts, healthy=${healthy}`);
  if (!healthy) {
    console.warn(`[Aviation] Systemic failure: ${failed}/${failed + succeeded} airports failed`);
  }
  return { alerts, healthy };
}
async function fetchSingleAirport(apiKey, airport) {
  try {
    const url = `${AVIATIONSTACK_URL}?access_key=${apiKey}&dep_iata=${airport.iata}&limit=100`;
    const resp = await fetch(url, {
      headers: { "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(5e3)
    });
    if (!resp.ok) {
      console.warn(`[Aviation] ${airport.iata}: HTTP ${resp.status}`);
      return { ok: false, alert: null };
    }
    const json = await resp.json();
    if (json.error) {
      console.warn(`[Aviation] ${airport.iata}: API error: ${json.error.message}`);
      return { ok: false, alert: null };
    }
    const flights = json?.data ?? [];
    const alert = aggregateFlights(airport, flights);
    if (flights.length > 0) {
      const cancelled = flights.filter((f) => f.flight_status === "cancelled").length;
      const delayed = flights.filter((f) => f.departure?.delay && f.departure.delay > 0).length;
      console.log(`[Aviation] ${airport.iata}: ${flights.length} flights, ${cancelled} cancelled, ${delayed} delayed \u2192 ${alert ? alert.severity.replace("FLIGHT_DELAY_SEVERITY_", "") : "normal"}`);
    }
    return { ok: true, alert };
  } catch (err) {
    console.warn(`[Aviation] ${airport.iata}: fetch error: ${err instanceof Error ? err.message : "unknown"}`);
    return { ok: false, alert: null };
  }
}
function aggregateFlights(airport, flights) {
  if (flights.length === 0) return null;
  let delayed = 0, cancelled = 0, totalDelay = 0;
  for (const f of flights) {
    if (f.flight_status === "cancelled") cancelled++;
    if (f.departure?.delay && f.departure.delay > 0) {
      delayed++;
      totalDelay += f.departure.delay;
    }
  }
  const total = flights.length;
  const cancelledPct = cancelled / total * 100;
  const delayedPct = delayed / total * 100;
  const avgDelay = delayed > 0 ? Math.round(totalDelay / delayed) : 0;
  let severity, delayType, reason;
  if (cancelledPct >= 80 && total >= MIN_FLIGHTS_FOR_CLOSURE) {
    severity = "severe";
    delayType = "closure";
    reason = "Airport closure / airspace restrictions";
  } else if (cancelledPct >= 50 && total >= MIN_FLIGHTS_FOR_CLOSURE) {
    severity = "major";
    delayType = "ground_stop";
    reason = `${Math.round(cancelledPct)}% flights cancelled`;
  } else if (cancelledPct >= 20 && total >= MIN_FLIGHTS_FOR_CLOSURE) {
    severity = "moderate";
    delayType = "ground_delay";
    reason = `${Math.round(cancelledPct)}% flights cancelled`;
  } else if (cancelledPct >= 10 && total >= MIN_FLIGHTS_FOR_CLOSURE) {
    severity = "minor";
    delayType = "general";
    reason = `${Math.round(cancelledPct)}% flights cancelled`;
  } else if (avgDelay > 0) {
    severity = determineSeverity(avgDelay, delayedPct);
    delayType = avgDelay >= 60 ? "ground_delay" : "general";
    reason = `Avg ${avgDelay}min delay, ${Math.round(delayedPct)}% delayed`;
  } else {
    return null;
  }
  if (severity === "normal") return null;
  return {
    id: `avstack-${airport.iata}`,
    iata: airport.iata,
    icao: airport.icao,
    name: airport.name,
    city: airport.city,
    country: airport.country,
    location: { latitude: airport.lat, longitude: airport.lon },
    region: toProtoRegion(airport.region),
    delayType: toProtoDelayType(delayType),
    severity: toProtoSeverity(severity),
    avgDelayMinutes: avgDelay,
    delayedFlightsPct: Math.round(delayedPct),
    cancelledFlights: cancelled,
    totalFlights: total,
    reason,
    source: toProtoSource("computed"),
    updatedAt: Date.now()
  };
}
function getRelayBaseUrl() {
  const relayUrl = process.env.WS_RELAY_URL;
  if (!relayUrl) return null;
  return relayUrl.replace("wss://", "https://").replace("ws://", "http://").replace(/\/$/, "");
}
function getRelayHeaders() {
  const headers = { "User-Agent": CHROME_UA };
  const relaySecret = process.env.RELAY_SHARED_SECRET;
  if (relaySecret) {
    const relayHeader = (process.env.RELAY_AUTH_HEADER || "x-relay-key").toLowerCase();
    headers[relayHeader] = relaySecret;
    headers.Authorization = `Bearer ${relaySecret}`;
  }
  return headers;
}
async function fetchNotamClosures(airports) {
  const apiKey = process.env.ICAO_API_KEY;
  const result = { closedIcaoCodes: /* @__PURE__ */ new Set(), notamsByIcao: /* @__PURE__ */ new Map() };
  if (!apiKey) {
    console.log("[Aviation] NOTAM: no ICAO_API_KEY \u2014 skipping");
    return result;
  }
  const relayBase = getRelayBaseUrl();
  const icaoCodes = airports.map((a) => a.icao);
  const now = Math.floor(Date.now() / 1e3);
  const locations = icaoCodes.join(",");
  let notams = [];
  try {
    if (relayBase) {
      const relayUrl = `${relayBase}/notam?locations=${encodeURIComponent(locations)}`;
      console.log(`[Aviation] NOTAM: fetching via relay for ${icaoCodes.length} airports`);
      const resp = await fetch(relayUrl, {
        headers: getRelayHeaders(),
        signal: AbortSignal.timeout(3e4)
      });
      if (!resp.ok) {
        console.warn(`[Aviation] NOTAM relay: HTTP ${resp.status}`);
        return result;
      }
      const data = await resp.json();
      if (Array.isArray(data)) notams = data;
    } else {
      console.log(`[Aviation] NOTAM: fetching direct for ${icaoCodes.length} airports`);
      const url = `${ICAO_NOTAM_URL}?api_key=${apiKey}&format=json&locations=${locations}`;
      const resp = await fetch(url, {
        headers: { "User-Agent": CHROME_UA },
        signal: AbortSignal.timeout(2e4)
      });
      if (!resp.ok) {
        console.warn(`[Aviation] NOTAM direct: HTTP ${resp.status}`);
        return result;
      }
      const contentType = resp.headers.get("content-type") || "";
      if (contentType.includes("text/html")) {
        console.warn("[Aviation] NOTAM direct: got HTML instead of JSON");
        return result;
      }
      const data = await resp.json();
      if (Array.isArray(data)) notams = data;
    }
  } catch (err) {
    console.warn(`[Aviation] NOTAM fetch: ${err instanceof Error ? err.message : "unknown"}`);
    return result;
  }
  console.log(`[Aviation] NOTAM: ${notams.length} raw NOTAMs received`);
  for (const n of notams) {
    const icao = n.itema || n.location || "";
    if (!icao || !icaoCodes.includes(icao)) continue;
    if (n.endvalidity && n.endvalidity < now) continue;
    const code23 = (n.code23 || "").toUpperCase();
    const code45 = (n.code45 || "").toUpperCase();
    const text = (n.iteme || "").toUpperCase();
    const isClosureCode = NOTAM_CLOSURE_QCODES.has(code23) && (code45 === "LC" || code45 === "AS" || code45 === "AU" || code45 === "XX" || code45 === "AW");
    const isClosureText = /\b(AD CLSD|AIRPORT CLOSED|AIRSPACE CLOSED|AD NOT AVBL|CLSD TO ALL)\b/.test(text);
    if (isClosureCode || isClosureText) {
      result.closedIcaoCodes.add(icao);
      result.notamsByIcao.set(icao, n.iteme || "Airport closure (NOTAM)");
    }
  }
  if (result.closedIcaoCodes.size > 0) {
    console.log(`[Aviation] NOTAM closures: ${[...result.closedIcaoCodes].join(", ")}`);
  } else {
    console.log("[Aviation] NOTAM: no closures found");
  }
  return result;
}
function buildNotamAlert(airport, reason) {
  return {
    id: `notam-${airport.iata}`,
    iata: airport.iata,
    icao: airport.icao,
    name: airport.name,
    city: airport.city,
    country: airport.country,
    location: { latitude: airport.lat, longitude: airport.lon },
    region: toProtoRegion(airport.region),
    delayType: toProtoDelayType("closure"),
    severity: toProtoSeverity("severe"),
    avgDelayMinutes: 0,
    delayedFlightsPct: 0,
    cancelledFlights: 0,
    totalFlights: 0,
    reason: reason.length > 200 ? reason.slice(0, 200) + "\u2026" : reason,
    source: toProtoSource("computed"),
    updatedAt: Date.now()
  };
}
function generateSimulatedDelay(airport) {
  const hour = (/* @__PURE__ */ new Date()).getUTCHours();
  const isRushHour = hour >= 6 && hour <= 10 || hour >= 16 && hour <= 20;
  const busyAirports = ["LHR", "CDG", "FRA", "JFK", "LAX", "ORD", "PEK", "HND", "DXB", "SIN"];
  const isBusy = busyAirports.includes(airport.iata);
  const random = Math.random();
  const delayChance = isRushHour ? 0.35 : 0.15;
  const hasDelay = random < (isBusy ? delayChance * 1.5 : delayChance);
  if (!hasDelay) return null;
  let avgDelayMinutes = 0;
  let delayType = "general";
  let reason = "Minor delays";
  const severityRoll = Math.random();
  if (severityRoll < 0.05) {
    avgDelayMinutes = 60 + Math.floor(Math.random() * 60);
    delayType = Math.random() < 0.3 ? "ground_stop" : "ground_delay";
    reason = Math.random() < 0.5 ? "Weather conditions" : "Air traffic volume";
  } else if (severityRoll < 0.2) {
    avgDelayMinutes = 45 + Math.floor(Math.random() * 20);
    delayType = "ground_delay";
    reason = Math.random() < 0.5 ? "Weather" : "High traffic volume";
  } else if (severityRoll < 0.5) {
    avgDelayMinutes = 25 + Math.floor(Math.random() * 20);
    delayType = Math.random() < 0.5 ? "departure_delay" : "arrival_delay";
    reason = "Congestion";
  } else {
    avgDelayMinutes = 15 + Math.floor(Math.random() * 15);
    delayType = "general";
    reason = "Minor delays";
  }
  const severity = determineSeverity(avgDelayMinutes);
  if (severity === "normal") return null;
  return {
    id: `sim-${airport.iata}`,
    iata: airport.iata,
    icao: airport.icao,
    name: airport.name,
    city: airport.city,
    country: airport.country,
    location: { latitude: airport.lat, longitude: airport.lon },
    region: toProtoRegion(airport.region),
    delayType: toProtoDelayType(delayType),
    severity: toProtoSeverity(severity),
    avgDelayMinutes,
    delayedFlightsPct: 0,
    cancelledFlights: 0,
    totalFlights: 0,
    reason,
    source: toProtoSource("computed"),
    updatedAt: Date.now()
  };
}

// server/worldmonitor/aviation/v1/list-airport-delays.ts
var FAA_CACHE_KEY = "aviation:delays:faa:v1";
var INTL_CACHE_KEY = "aviation:delays:intl:v3";
var NOTAM_CACHE_KEY = "aviation:notam:closures:v1";
var CACHE_TTL2 = 7200;
async function listAirportDelays(_ctx, _req) {
  const t0 = Date.now();
  let faaAlerts = [];
  try {
    const result = await cachedFetchJson(
      FAA_CACHE_KEY,
      CACHE_TTL2,
      async () => {
        const alerts = [];
        const faaResponse = await fetch(FAA_URL, {
          headers: { Accept: "application/xml", "User-Agent": CHROME_UA },
          signal: AbortSignal.timeout(15e3)
        });
        let faaDelays = /* @__PURE__ */ new Map();
        if (faaResponse.ok) {
          const xml = await faaResponse.text();
          faaDelays = parseFaaXml(xml);
        }
        for (const iata of FAA_AIRPORTS) {
          const airport = MONITORED_AIRPORTS.find((a) => a.iata === iata);
          if (!airport) continue;
          const faaDelay = faaDelays.get(iata);
          if (faaDelay) {
            alerts.push({
              id: `faa-${iata}`,
              iata,
              icao: airport.icao,
              name: airport.name,
              city: airport.city,
              country: airport.country,
              location: { latitude: airport.lat, longitude: airport.lon },
              region: toProtoRegion(airport.region),
              delayType: toProtoDelayType(faaDelay.type),
              severity: toProtoSeverity(determineSeverity(faaDelay.avgDelay)),
              avgDelayMinutes: faaDelay.avgDelay,
              delayedFlightsPct: 0,
              cancelledFlights: 0,
              totalFlights: 0,
              reason: faaDelay.reason,
              source: toProtoSource("faa"),
              updatedAt: Date.now()
            });
          }
        }
        return { alerts };
      }
    );
    faaAlerts = result?.alerts ?? [];
    console.log(`[Aviation] FAA: ${faaAlerts.length} alerts`);
  } catch (err) {
    console.warn(`[Aviation] FAA fetch failed: ${err instanceof Error ? err.message : "unknown"}`);
  }
  let intlAlerts = [];
  try {
    const result = await cachedFetchJson(
      INTL_CACHE_KEY,
      CACHE_TTL2,
      async () => {
        const nonUs = MONITORED_AIRPORTS.filter((a) => a.country !== "USA");
        const apiKey = process.env.AVIATIONSTACK_API;
        if (!apiKey) {
          console.log("[Aviation] No AVIATIONSTACK_API key \u2014 using simulation");
          const sim = nonUs.map((a) => generateSimulatedDelay(a)).filter(Boolean);
          return { alerts: sim };
        }
        const avResult = await fetchAviationStackDelays(nonUs);
        if (!avResult.healthy) {
          console.warn("[Aviation] AviationStack unhealthy \u2014 falling back to simulation");
          const sim = nonUs.map((a) => generateSimulatedDelay(a)).filter(Boolean);
          return { alerts: sim };
        }
        console.log(`[Aviation] AviationStack OK: ${avResult.alerts.length} real alerts`);
        return { alerts: avResult.alerts };
      }
    );
    intlAlerts = result?.alerts ?? [];
    console.log(`[Aviation] Intl: ${intlAlerts.length} alerts`);
  } catch (err) {
    console.warn(`[Aviation] Intl fetch failed: ${err instanceof Error ? err.message : "unknown"}`);
  }
  let allAlerts = [...faaAlerts, ...intlAlerts];
  if (process.env.ICAO_API_KEY) {
    try {
      const notamResult = await cachedFetchJson(
        NOTAM_CACHE_KEY,
        CACHE_TTL2,
        async () => {
          const mena = MONITORED_AIRPORTS.filter((a) => a.region === "mena");
          const result = await fetchNotamClosures(mena);
          const closedIcaos = [...result.closedIcaoCodes];
          const reasons = {};
          for (const [icao, reason] of result.notamsByIcao) reasons[icao] = reason;
          return { closedIcaos, reasons };
        }
      );
      if (notamResult && notamResult.closedIcaos.length > 0) {
        const existingIatas = new Set(allAlerts.map((a) => a.iata));
        for (const icao of notamResult.closedIcaos) {
          const airport = MONITORED_AIRPORTS.find((a) => a.icao === icao);
          if (!airport) continue;
          const reason = notamResult.reasons[icao] || "Airport closure (NOTAM)";
          if (existingIatas.has(airport.iata)) {
            const idx = allAlerts.findIndex((a) => a.iata === airport.iata);
            if (idx >= 0) {
              allAlerts[idx] = buildNotamAlert(airport, reason);
            }
          } else {
            allAlerts.push(buildNotamAlert(airport, reason));
          }
        }
        console.log(`[Aviation] NOTAM: ${notamResult.closedIcaos.length} closures applied`);
      }
    } catch (err) {
      console.warn(`[Aviation] NOTAM fetch failed: ${err instanceof Error ? err.message : "unknown"}`);
    }
  }
  const alertedIatas = new Set(allAlerts.map((a) => a.iata));
  let normalCount = 0;
  for (const airport of MONITORED_AIRPORTS) {
    if (!alertedIatas.has(airport.iata)) {
      normalCount++;
      allAlerts.push({
        id: `status-${airport.iata}`,
        iata: airport.iata,
        icao: airport.icao,
        name: airport.name,
        city: airport.city,
        country: airport.country,
        location: { latitude: airport.lat, longitude: airport.lon },
        region: toProtoRegion(airport.region),
        delayType: toProtoDelayType("general"),
        severity: toProtoSeverity("normal"),
        avgDelayMinutes: 0,
        delayedFlightsPct: 0,
        cancelledFlights: 0,
        totalFlights: 0,
        reason: "Normal operations",
        source: toProtoSource("computed"),
        updatedAt: Date.now()
      });
    }
  }
  console.log(`[Aviation] Total: ${allAlerts.length} alerts (${normalCount} normal) in ${Date.now() - t0}ms`);
  return { alerts: allAlerts };
}

// server/worldmonitor/aviation/v1/handler.ts
var aviationHandler = {
  listAirportDelays
};

// src/generated/server/worldmonitor/research/v1/service_server.ts
var ValidationError7 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createResearchServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/research/v1/list-arxiv-papers",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            category: params.get("category") ?? "",
            query: params.get("query") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listArxivPapers", body);
            if (bodyViolations) {
              throw new ValidationError7(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listArxivPapers(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError7) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/research/v1/list-trending-repos",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            language: params.get("language") ?? "",
            period: params.get("period") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listTrendingRepos", body);
            if (bodyViolations) {
              throw new ValidationError7(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listTrendingRepos(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError7) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/research/v1/list-hackernews-items",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            feedType: params.get("feed_type") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listHackernewsItems", body);
            if (bodyViolations) {
              throw new ValidationError7(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listHackernewsItems(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError7) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/research/v1/list-tech-events",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            type: params.get("type") ?? "",
            mappable: params.get("mappable") === "true",
            limit: Number(params.get("limit") ?? "0"),
            days: Number(params.get("days") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listTechEvents", body);
            if (bodyViolations) {
              throw new ValidationError7(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listTechEvents(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError7) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/research/v1/list-arxiv-papers.ts
var REDIS_CACHE_KEY5 = "research:arxiv:v1";
var REDIS_CACHE_TTL5 = 3600;
var xmlParser2 = new XMLParser({
  ignoreAttributes: false,
  // CRITICAL: arXiv uses attributes for category term, link href/rel
  attributeNamePrefix: "@_",
  isArray: (_name, jpath) => /\.(entry|author|category|link)$/.test(jpath)
});
async function fetchArxivPapers(req) {
  const category = req.category || "cs.AI";
  const pageSize = req.pageSize || 50;
  let searchQuery;
  if (req.query) {
    searchQuery = `all:${req.query}+AND+cat:${category}`;
  } else {
    searchQuery = `cat:${category}`;
  }
  const url = `https://export.arxiv.org/api/query?search_query=${searchQuery}&start=0&max_results=${pageSize}`;
  const response = await fetch(url, {
    headers: { Accept: "application/xml", "User-Agent": CHROME_UA },
    signal: AbortSignal.timeout(15e3)
  });
  if (!response.ok) return [];
  const xml = await response.text();
  const parsed = xmlParser2.parse(xml);
  const feed = parsed?.feed;
  if (!feed) return [];
  const entries = Array.isArray(feed.entry) ? feed.entry : feed.entry ? [feed.entry] : [];
  return entries.map((entry) => {
    const rawId = String(entry.id || "");
    const id = rawId.split("/").pop() || rawId;
    const title = (entry.title || "").trim().replace(/\s+/g, " ");
    const summary = (entry.summary || "").trim().replace(/\s+/g, " ");
    const authors = (entry.author ?? []).map((a) => a.name || "");
    const categories = (entry.category ?? []).map((c) => c["@_term"] || "");
    const publishedAt = entry.published ? new Date(entry.published).getTime() : 0;
    const links = Array.isArray(entry.link) ? entry.link : entry.link ? [entry.link] : [];
    const alternateLink = links.find((l) => l["@_rel"] === "alternate");
    const url2 = alternateLink?.["@_href"] || rawId;
    return { id, title, summary, authors, categories, publishedAt, url: url2 };
  });
}
async function listArxivPapers(_ctx, req) {
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY5}:${req.category || "cs.AI"}:${req.query || ""}:${req.pageSize || 50}`;
    const result = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL5,
      async () => {
        const papers = await fetchArxivPapers(req);
        return papers.length > 0 ? { papers, pagination: void 0 } : null;
      }
    );
    return result || { papers: [], pagination: void 0 };
  } catch {
    return { papers: [], pagination: void 0 };
  }
}

// server/worldmonitor/research/v1/list-trending-repos.ts
var REDIS_CACHE_KEY6 = "research:trending:v1";
var REDIS_CACHE_TTL6 = 3600;
async function fetchTrendingRepos(req) {
  const language = req.language || "python";
  const period = req.period || "daily";
  const pageSize = req.pageSize || 50;
  const primaryUrl = `https://api.gitterapp.com/repositories?language=${language}&since=${period}`;
  let data;
  try {
    const response = await fetch(primaryUrl, {
      headers: { Accept: "application/json", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(1e4)
    });
    if (!response.ok) throw new Error("Primary API failed");
    data = await response.json();
  } catch {
    try {
      const fallbackUrl = `https://gh-trending-api.herokuapp.com/repositories/${language}?since=${period}`;
      const fallbackResponse = await fetch(fallbackUrl, {
        headers: { Accept: "application/json", "User-Agent": CHROME_UA },
        signal: AbortSignal.timeout(1e4)
      });
      if (!fallbackResponse.ok) return [];
      data = await fallbackResponse.json();
    } catch {
      return [];
    }
  }
  if (!Array.isArray(data)) return [];
  return data.slice(0, pageSize).map((raw) => ({
    fullName: `${raw.author}/${raw.name}`,
    description: raw.description || "",
    language: raw.language || "",
    stars: raw.stars || 0,
    starsToday: raw.currentPeriodStars || 0,
    forks: raw.forks || 0,
    url: raw.url || `https://github.com/${raw.author}/${raw.name}`
  }));
}
async function listTrendingRepos(_ctx, req) {
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY6}:${req.language || "python"}:${req.period || "daily"}:${req.pageSize || 50}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL6, async () => {
      const repos = await fetchTrendingRepos(req);
      return repos.length > 0 ? { repos, pagination: void 0 } : null;
    });
    return result || { repos: [], pagination: void 0 };
  } catch {
    return { repos: [], pagination: void 0 };
  }
}

// server/worldmonitor/research/v1/list-hackernews-items.ts
var REDIS_CACHE_KEY7 = "research:hackernews:v1";
var REDIS_CACHE_TTL7 = 600;
var ALLOWED_HN_FEEDS = /* @__PURE__ */ new Set(["top", "new", "best", "ask", "show", "job"]);
var HN_MAX_CONCURRENCY = 10;
async function fetchHackernewsItems(req) {
  const feedType = ALLOWED_HN_FEEDS.has(req.feedType) ? req.feedType : "top";
  const pageSize = req.pageSize || 30;
  const idsUrl = `https://hacker-news.firebaseio.com/v0/${feedType}stories.json`;
  const idsResponse = await fetch(idsUrl, {
    headers: { "User-Agent": CHROME_UA },
    signal: AbortSignal.timeout(1e4)
  });
  if (!idsResponse.ok) return [];
  const allIds = await idsResponse.json();
  if (!Array.isArray(allIds)) return [];
  const ids = allIds.slice(0, pageSize);
  const items = [];
  for (let i = 0; i < ids.length; i += HN_MAX_CONCURRENCY) {
    const batch = ids.slice(i, i + HN_MAX_CONCURRENCY);
    const results = await Promise.all(
      batch.map(async (id) => {
        try {
          const res = await fetch(
            `https://hacker-news.firebaseio.com/v0/item/${id}.json`,
            { headers: { "User-Agent": CHROME_UA }, signal: AbortSignal.timeout(5e3) }
          );
          if (!res.ok) return null;
          const raw = await res.json();
          if (!raw || raw.type !== "story") return null;
          return {
            id: raw.id || 0,
            title: raw.title || "",
            url: raw.url || "",
            score: raw.score || 0,
            commentCount: raw.descendants || 0,
            by: raw.by || "",
            submittedAt: (raw.time || 0) * 1e3
            // HN uses Unix seconds, proto uses ms
          };
        } catch {
          return null;
        }
      })
    );
    for (const item of results) {
      if (item) items.push(item);
    }
  }
  return items;
}
async function listHackernewsItems(_ctx, req) {
  try {
    const feedType = ALLOWED_HN_FEEDS.has(req.feedType) ? req.feedType : "top";
    const cacheKey2 = `${REDIS_CACHE_KEY7}:${feedType}:${req.pageSize || 30}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL7, async () => {
      const items = await fetchHackernewsItems(req);
      return items.length > 0 ? { items, pagination: void 0 } : null;
    });
    return result || { items: [], pagination: void 0 };
  } catch {
    return { items: [], pagination: void 0 };
  }
}

// api/data/city-coords.ts
var CITY_COORDS = {
  // North America - USA
  "san francisco": { lat: 37.7749, lng: -122.4194, country: "USA" },
  "san jose": { lat: 37.3382, lng: -121.8863, country: "USA" },
  "palo alto": { lat: 37.4419, lng: -122.143, country: "USA" },
  "mountain view": { lat: 37.3861, lng: -122.0839, country: "USA" },
  "menlo park": { lat: 37.453, lng: -122.1817, country: "USA" },
  "cupertino": { lat: 37.323, lng: -122.0322, country: "USA" },
  "sunnyvale": { lat: 37.3688, lng: -122.0363, country: "USA" },
  "santa clara": { lat: 37.3541, lng: -121.9552, country: "USA" },
  "redwood city": { lat: 37.4852, lng: -122.2364, country: "USA" },
  "oakland": { lat: 37.8044, lng: -122.2712, country: "USA" },
  "berkeley": { lat: 37.8716, lng: -122.2727, country: "USA" },
  "los angeles": { lat: 34.0522, lng: -118.2437, country: "USA" },
  "santa monica": { lat: 34.0195, lng: -118.4912, country: "USA" },
  "pasadena": { lat: 34.1478, lng: -118.1445, country: "USA" },
  "irvine": { lat: 33.6846, lng: -117.8265, country: "USA" },
  "san diego": { lat: 32.7157, lng: -117.1611, country: "USA" },
  "seattle": { lat: 47.6062, lng: -122.3321, country: "USA" },
  "bellevue": { lat: 47.6101, lng: -122.2015, country: "USA" },
  "redmond": { lat: 47.674, lng: -122.1215, country: "USA" },
  "portland": { lat: 45.5155, lng: -122.6789, country: "USA" },
  "new york": { lat: 40.7128, lng: -74.006, country: "USA" },
  "nyc": { lat: 40.7128, lng: -74.006, country: "USA" },
  "manhattan": { lat: 40.7831, lng: -73.9712, country: "USA" },
  "brooklyn": { lat: 40.6782, lng: -73.9442, country: "USA" },
  "boston": { lat: 42.3601, lng: -71.0589, country: "USA" },
  "cambridge": { lat: 42.3736, lng: -71.1097, country: "USA" },
  "chicago": { lat: 41.8781, lng: -87.6298, country: "USA" },
  "austin": { lat: 30.2672, lng: -97.7431, country: "USA" },
  "austin, tx": { lat: 30.2672, lng: -97.7431, country: "USA" },
  "dallas": { lat: 32.7767, lng: -96.797, country: "USA" },
  "houston": { lat: 29.7604, lng: -95.3698, country: "USA" },
  "denver": { lat: 39.7392, lng: -104.9903, country: "USA" },
  "boulder": { lat: 40.015, lng: -105.2705, country: "USA" },
  "phoenix": { lat: 33.4484, lng: -112.074, country: "USA" },
  "scottsdale": { lat: 33.4942, lng: -111.9261, country: "USA" },
  "miami": { lat: 25.7617, lng: -80.1918, country: "USA" },
  "orlando": { lat: 28.5383, lng: -81.3792, country: "USA" },
  "tampa": { lat: 27.9506, lng: -82.4572, country: "USA" },
  "atlanta": { lat: 33.749, lng: -84.388, country: "USA" },
  "washington": { lat: 38.9072, lng: -77.0369, country: "USA" },
  "washington dc": { lat: 38.9072, lng: -77.0369, country: "USA" },
  "washington, dc": { lat: 38.9072, lng: -77.0369, country: "USA" },
  "dc": { lat: 38.9072, lng: -77.0369, country: "USA" },
  "reston": { lat: 38.9586, lng: -77.357, country: "USA" },
  "philadelphia": { lat: 39.9526, lng: -75.1652, country: "USA" },
  "pittsburgh": { lat: 40.4406, lng: -79.9959, country: "USA" },
  "detroit": { lat: 42.3314, lng: -83.0458, country: "USA" },
  "ann arbor": { lat: 42.2808, lng: -83.743, country: "USA" },
  "minneapolis": { lat: 44.9778, lng: -93.265, country: "USA" },
  "salt lake city": { lat: 40.7608, lng: -111.891, country: "USA" },
  "las vegas": { lat: 36.1699, lng: -115.1398, country: "USA" },
  "raleigh": { lat: 35.7796, lng: -78.6382, country: "USA" },
  "durham": { lat: 35.994, lng: -78.8986, country: "USA" },
  "chapel hill": { lat: 35.9132, lng: -79.0558, country: "USA" },
  "charlotte": { lat: 35.2271, lng: -80.8431, country: "USA" },
  "nashville": { lat: 36.1627, lng: -86.7816, country: "USA" },
  "indianapolis": { lat: 39.7684, lng: -86.1581, country: "USA" },
  "columbus": { lat: 39.9612, lng: -82.9988, country: "USA" },
  "cleveland": { lat: 41.4993, lng: -81.6944, country: "USA" },
  "cincinnati": { lat: 39.1031, lng: -84.512, country: "USA" },
  "st. louis": { lat: 38.627, lng: -90.1994, country: "USA" },
  "kansas city": { lat: 39.0997, lng: -94.5786, country: "USA" },
  "omaha": { lat: 41.2565, lng: -95.9345, country: "USA" },
  "milwaukee": { lat: 43.0389, lng: -87.9065, country: "USA" },
  "new orleans": { lat: 29.9511, lng: -90.0715, country: "USA" },
  "san antonio": { lat: 29.4241, lng: -98.4936, country: "USA" },
  "albuquerque": { lat: 35.0844, lng: -106.6504, country: "USA" },
  "tucson": { lat: 32.2226, lng: -110.9747, country: "USA" },
  "honolulu": { lat: 21.3069, lng: -157.8583, country: "USA" },
  "anchorage": { lat: 61.2181, lng: -149.9003, country: "USA" },
  // North America - Canada
  "toronto": { lat: 43.6532, lng: -79.3832, country: "Canada" },
  "vancouver": { lat: 49.2827, lng: -123.1207, country: "Canada" },
  "montreal": { lat: 45.5017, lng: -73.5673, country: "Canada" },
  "ottawa": { lat: 45.4215, lng: -75.6972, country: "Canada" },
  "calgary": { lat: 51.0447, lng: -114.0719, country: "Canada" },
  "edmonton": { lat: 53.5461, lng: -113.4938, country: "Canada" },
  "winnipeg": { lat: 49.8951, lng: -97.1384, country: "Canada" },
  "quebec city": { lat: 46.8139, lng: -71.208, country: "Canada" },
  "waterloo": { lat: 43.4643, lng: -80.5204, country: "Canada" },
  "victoria": { lat: 48.4284, lng: -123.3656, country: "Canada" },
  "halifax": { lat: 44.6488, lng: -63.5752, country: "Canada" },
  // Mexico & Central America
  "mexico city": { lat: 19.4326, lng: -99.1332, country: "Mexico" },
  "guadalajara": { lat: 20.6597, lng: -103.3496, country: "Mexico" },
  "monterrey": { lat: 25.6866, lng: -100.3161, country: "Mexico" },
  "tijuana": { lat: 32.5149, lng: -117.0382, country: "Mexico" },
  "cancun": { lat: 21.1619, lng: -86.8515, country: "Mexico" },
  "panama city": { lat: 8.9824, lng: -79.5199, country: "Panama" },
  "san jose cr": { lat: 9.9281, lng: -84.0907, country: "Costa Rica" },
  // South America
  "sao paulo": { lat: -23.5505, lng: -46.6333, country: "Brazil" },
  "s\xE3o paulo": { lat: -23.5505, lng: -46.6333, country: "Brazil" },
  "rio de janeiro": { lat: -22.9068, lng: -43.1729, country: "Brazil" },
  "brasilia": { lat: -15.7975, lng: -47.8919, country: "Brazil" },
  "belo horizonte": { lat: -19.9167, lng: -43.9345, country: "Brazil" },
  "porto alegre": { lat: -30.0346, lng: -51.2177, country: "Brazil" },
  "buenos aires": { lat: -34.6037, lng: -58.3816, country: "Argentina" },
  "santiago": { lat: -33.4489, lng: -70.6693, country: "Chile" },
  "bogota": { lat: 4.711, lng: -74.0721, country: "Colombia" },
  "bogot\xE1": { lat: 4.711, lng: -74.0721, country: "Colombia" },
  "medellin": { lat: 6.2476, lng: -75.5658, country: "Colombia" },
  "medell\xEDn": { lat: 6.2476, lng: -75.5658, country: "Colombia" },
  "lima": { lat: -12.0464, lng: -77.0428, country: "Peru" },
  "caracas": { lat: 10.4806, lng: -66.9036, country: "Venezuela" },
  "montevideo": { lat: -34.9011, lng: -56.1645, country: "Uruguay" },
  "quito": { lat: -0.1807, lng: -78.4678, country: "Ecuador" },
  // Europe - UK & Ireland
  "london": { lat: 51.5074, lng: -0.1278, country: "UK" },
  "cambridge uk": { lat: 52.2053, lng: 0.1218, country: "UK" },
  "oxford": { lat: 51.752, lng: -1.2577, country: "UK" },
  "manchester": { lat: 53.4808, lng: -2.2426, country: "UK" },
  "birmingham": { lat: 52.4862, lng: -1.8904, country: "UK" },
  "edinburgh": { lat: 55.9533, lng: -3.1883, country: "UK" },
  "glasgow": { lat: 55.8642, lng: -4.2518, country: "UK" },
  "bristol": { lat: 51.4545, lng: -2.5879, country: "UK" },
  "leeds": { lat: 53.8008, lng: -1.5491, country: "UK" },
  "liverpool": { lat: 53.4084, lng: -2.9916, country: "UK" },
  "belfast": { lat: 54.5973, lng: -5.9301, country: "UK" },
  "cardiff": { lat: 51.4816, lng: -3.1791, country: "UK" },
  "dublin": { lat: 53.3498, lng: -6.2603, country: "Ireland" },
  "cork": { lat: 51.8985, lng: -8.4756, country: "Ireland" },
  "galway": { lat: 53.2707, lng: -9.0568, country: "Ireland" },
  // Europe - Western
  "paris": { lat: 48.8566, lng: 2.3522, country: "France" },
  "lyon": { lat: 45.764, lng: 4.8357, country: "France" },
  "marseille": { lat: 43.2965, lng: 5.3698, country: "France" },
  "toulouse": { lat: 43.6047, lng: 1.4442, country: "France" },
  "nice": { lat: 43.7102, lng: 7.262, country: "France" },
  "bordeaux": { lat: 44.8378, lng: -0.5792, country: "France" },
  "strasbourg": { lat: 48.5734, lng: 7.7521, country: "France" },
  "nantes": { lat: 47.2184, lng: -1.5536, country: "France" },
  "cannes": { lat: 43.5528, lng: 7.0174, country: "France" },
  "monaco": { lat: 43.7384, lng: 7.4246, country: "Monaco" },
  "berlin": { lat: 52.52, lng: 13.405, country: "Germany" },
  "munich": { lat: 48.1351, lng: 11.582, country: "Germany" },
  "m\xFCnchen": { lat: 48.1351, lng: 11.582, country: "Germany" },
  "frankfurt": { lat: 50.1109, lng: 8.6821, country: "Germany" },
  "hamburg": { lat: 53.5511, lng: 9.9937, country: "Germany" },
  "cologne": { lat: 50.9375, lng: 6.9603, country: "Germany" },
  "k\xF6ln": { lat: 50.9375, lng: 6.9603, country: "Germany" },
  "d\xFCsseldorf": { lat: 51.2277, lng: 6.7735, country: "Germany" },
  "dusseldorf": { lat: 51.2277, lng: 6.7735, country: "Germany" },
  "stuttgart": { lat: 48.7758, lng: 9.1829, country: "Germany" },
  "hanover": { lat: 52.3759, lng: 9.732, country: "Germany" },
  "hannover": { lat: 52.3759, lng: 9.732, country: "Germany" },
  "dresden": { lat: 51.0504, lng: 13.7373, country: "Germany" },
  "leipzig": { lat: 51.3397, lng: 12.3731, country: "Germany" },
  "nuremberg": { lat: 49.4521, lng: 11.0767, country: "Germany" },
  "amsterdam": { lat: 52.3676, lng: 4.9041, country: "Netherlands" },
  "rotterdam": { lat: 51.9225, lng: 4.4792, country: "Netherlands" },
  "the hague": { lat: 52.0705, lng: 4.3007, country: "Netherlands" },
  "eindhoven": { lat: 51.4416, lng: 5.4697, country: "Netherlands" },
  "utrecht": { lat: 52.0907, lng: 5.1214, country: "Netherlands" },
  "brussels": { lat: 50.8503, lng: 4.3517, country: "Belgium" },
  "antwerp": { lat: 51.2194, lng: 4.4025, country: "Belgium" },
  "ghent": { lat: 51.0543, lng: 3.7174, country: "Belgium" },
  "luxembourg": { lat: 49.6116, lng: 6.1319, country: "Luxembourg" },
  "zurich": { lat: 47.3769, lng: 8.5417, country: "Switzerland" },
  "z\xFCrich": { lat: 47.3769, lng: 8.5417, country: "Switzerland" },
  "geneva": { lat: 46.2044, lng: 6.1432, country: "Switzerland" },
  "gen\xE8ve": { lat: 46.2044, lng: 6.1432, country: "Switzerland" },
  "basel": { lat: 47.5596, lng: 7.5886, country: "Switzerland" },
  "bern": { lat: 46.948, lng: 7.4474, country: "Switzerland" },
  "lausanne": { lat: 46.5197, lng: 6.6323, country: "Switzerland" },
  "davos": { lat: 46.8027, lng: 9.836, country: "Switzerland" },
  "vienna": { lat: 48.2082, lng: 16.3738, country: "Austria" },
  "wien": { lat: 48.2082, lng: 16.3738, country: "Austria" },
  "salzburg": { lat: 47.8095, lng: 13.055, country: "Austria" },
  "graz": { lat: 47.0707, lng: 15.4395, country: "Austria" },
  "innsbruck": { lat: 47.2692, lng: 11.4041, country: "Austria" },
  // Europe - Southern
  "barcelona": { lat: 41.3851, lng: 2.1734, country: "Spain" },
  "madrid": { lat: 40.4168, lng: -3.7038, country: "Spain" },
  "valencia": { lat: 39.4699, lng: -0.3763, country: "Spain" },
  "seville": { lat: 37.3891, lng: -5.9845, country: "Spain" },
  "sevilla": { lat: 37.3891, lng: -5.9845, country: "Spain" },
  "malaga": { lat: 36.7213, lng: -4.4214, country: "Spain" },
  "m\xE1laga": { lat: 36.7213, lng: -4.4214, country: "Spain" },
  "bilbao": { lat: 43.263, lng: -2.935, country: "Spain" },
  "lisbon": { lat: 38.7223, lng: -9.1393, country: "Portugal" },
  "lisboa": { lat: 38.7223, lng: -9.1393, country: "Portugal" },
  "porto": { lat: 41.1579, lng: -8.6291, country: "Portugal" },
  "rome": { lat: 41.9028, lng: 12.4964, country: "Italy" },
  "roma": { lat: 41.9028, lng: 12.4964, country: "Italy" },
  "milan": { lat: 45.4642, lng: 9.19, country: "Italy" },
  "milano": { lat: 45.4642, lng: 9.19, country: "Italy" },
  "florence": { lat: 43.7696, lng: 11.2558, country: "Italy" },
  "firenze": { lat: 43.7696, lng: 11.2558, country: "Italy" },
  "venice": { lat: 45.4408, lng: 12.3155, country: "Italy" },
  "venezia": { lat: 45.4408, lng: 12.3155, country: "Italy" },
  "turin": { lat: 45.0703, lng: 7.6869, country: "Italy" },
  "torino": { lat: 45.0703, lng: 7.6869, country: "Italy" },
  "naples": { lat: 40.8518, lng: 14.2681, country: "Italy" },
  "napoli": { lat: 40.8518, lng: 14.2681, country: "Italy" },
  "bologna": { lat: 44.4949, lng: 11.3426, country: "Italy" },
  "athens": { lat: 37.9838, lng: 23.7275, country: "Greece" },
  "thessaloniki": { lat: 40.6401, lng: 22.9444, country: "Greece" },
  "malta": { lat: 35.8989, lng: 14.5146, country: "Malta" },
  "valletta": { lat: 35.8989, lng: 14.5146, country: "Malta" },
  // Europe - Northern
  "stockholm": { lat: 59.3293, lng: 18.0686, country: "Sweden" },
  "gothenburg": { lat: 57.7089, lng: 11.9746, country: "Sweden" },
  "g\xF6teborg": { lat: 57.7089, lng: 11.9746, country: "Sweden" },
  "malm\xF6": { lat: 55.605, lng: 13.0038, country: "Sweden" },
  "malmo": { lat: 55.605, lng: 13.0038, country: "Sweden" },
  "copenhagen": { lat: 55.6761, lng: 12.5683, country: "Denmark" },
  "k\xF8benhavn": { lat: 55.6761, lng: 12.5683, country: "Denmark" },
  "aarhus": { lat: 56.1629, lng: 10.2039, country: "Denmark" },
  "oslo": { lat: 59.9139, lng: 10.7522, country: "Norway" },
  "bergen": { lat: 60.3913, lng: 5.3221, country: "Norway" },
  "helsinki": { lat: 60.1699, lng: 24.9384, country: "Finland" },
  "espoo": { lat: 60.2055, lng: 24.6559, country: "Finland" },
  "tampere": { lat: 61.4978, lng: 23.761, country: "Finland" },
  "reykjavik": { lat: 64.1466, lng: -21.9426, country: "Iceland" },
  // Europe - Eastern
  "warsaw": { lat: 52.2297, lng: 21.0122, country: "Poland" },
  "warszawa": { lat: 52.2297, lng: 21.0122, country: "Poland" },
  "krakow": { lat: 50.0647, lng: 19.945, country: "Poland" },
  "krak\xF3w": { lat: 50.0647, lng: 19.945, country: "Poland" },
  "wroclaw": { lat: 51.1079, lng: 17.0385, country: "Poland" },
  "wroc\u0142aw": { lat: 51.1079, lng: 17.0385, country: "Poland" },
  "gdansk": { lat: 54.352, lng: 18.6466, country: "Poland" },
  "prague": { lat: 50.0755, lng: 14.4378, country: "Czech Republic" },
  "praha": { lat: 50.0755, lng: 14.4378, country: "Czech Republic" },
  "brno": { lat: 49.1951, lng: 16.6068, country: "Czech Republic" },
  "budapest": { lat: 47.4979, lng: 19.0402, country: "Hungary" },
  "bucharest": { lat: 44.4268, lng: 26.1025, country: "Romania" },
  "bucure\u0219ti": { lat: 44.4268, lng: 26.1025, country: "Romania" },
  "cluj-napoca": { lat: 46.7712, lng: 23.6236, country: "Romania" },
  "sofia": { lat: 42.6977, lng: 23.3219, country: "Bulgaria" },
  "belgrade": { lat: 44.7866, lng: 20.4489, country: "Serbia" },
  "beograd": { lat: 44.7866, lng: 20.4489, country: "Serbia" },
  "zagreb": { lat: 45.815, lng: 15.9819, country: "Croatia" },
  "ljubljana": { lat: 46.0569, lng: 14.5058, country: "Slovenia" },
  "bratislava": { lat: 48.1486, lng: 17.1077, country: "Slovakia" },
  "tallinn": { lat: 59.437, lng: 24.7536, country: "Estonia" },
  "riga": { lat: 56.9496, lng: 24.1052, country: "Latvia" },
  "vilnius": { lat: 54.6872, lng: 25.2797, country: "Lithuania" },
  "kyiv": { lat: 50.4501, lng: 30.5234, country: "Ukraine" },
  "kiev": { lat: 50.4501, lng: 30.5234, country: "Ukraine" },
  "lviv": { lat: 49.8397, lng: 24.0297, country: "Ukraine" },
  "minsk": { lat: 53.9045, lng: 27.5615, country: "Belarus" },
  "moscow": { lat: 55.7558, lng: 37.6173, country: "Russia" },
  "st. petersburg": { lat: 59.9311, lng: 30.3609, country: "Russia" },
  "saint petersburg": { lat: 59.9311, lng: 30.3609, country: "Russia" },
  // Middle East
  "dubai": { lat: 25.2048, lng: 55.2708, country: "UAE" },
  "abu dhabi": { lat: 24.4539, lng: 54.3773, country: "UAE" },
  "doha": { lat: 25.2854, lng: 51.531, country: "Qatar" },
  "riyadh": { lat: 24.7136, lng: 46.6753, country: "Saudi Arabia" },
  "jeddah": { lat: 21.4858, lng: 39.1925, country: "Saudi Arabia" },
  "neom": { lat: 28, lng: 35, country: "Saudi Arabia" },
  "tel aviv": { lat: 32.0853, lng: 34.7818, country: "Israel" },
  "jerusalem": { lat: 31.7683, lng: 35.2137, country: "Israel" },
  "haifa": { lat: 32.794, lng: 34.9896, country: "Israel" },
  "amman": { lat: 31.9454, lng: 35.9284, country: "Jordan" },
  "beirut": { lat: 33.8938, lng: 35.5018, country: "Lebanon" },
  "istanbul": { lat: 41.0082, lng: 28.9784, country: "Turkey" },
  "ankara": { lat: 39.9334, lng: 32.8597, country: "Turkey" },
  "izmir": { lat: 38.4237, lng: 27.1428, country: "Turkey" },
  "tehran": { lat: 35.6892, lng: 51.389, country: "Iran" },
  "cairo": { lat: 30.0444, lng: 31.2357, country: "Egypt" },
  "muscat": { lat: 23.588, lng: 58.3829, country: "Oman" },
  "manama": { lat: 26.2285, lng: 50.586, country: "Bahrain" },
  "kuwait city": { lat: 29.3759, lng: 47.9774, country: "Kuwait" },
  // Asia - East
  "tokyo": { lat: 35.6762, lng: 139.6503, country: "Japan" },
  "osaka": { lat: 34.6937, lng: 135.5023, country: "Japan" },
  "kyoto": { lat: 35.0116, lng: 135.7681, country: "Japan" },
  "yokohama": { lat: 35.4437, lng: 139.638, country: "Japan" },
  "nagoya": { lat: 35.1815, lng: 136.9066, country: "Japan" },
  "fukuoka": { lat: 33.5904, lng: 130.4017, country: "Japan" },
  "sapporo": { lat: 43.0618, lng: 141.3545, country: "Japan" },
  "kobe": { lat: 34.6901, lng: 135.1956, country: "Japan" },
  "seoul": { lat: 37.5665, lng: 126.978, country: "South Korea" },
  "busan": { lat: 35.1796, lng: 129.0756, country: "South Korea" },
  "incheon": { lat: 37.4563, lng: 126.7052, country: "South Korea" },
  "beijing": { lat: 39.9042, lng: 116.4074, country: "China" },
  "shanghai": { lat: 31.2304, lng: 121.4737, country: "China" },
  "shenzhen": { lat: 22.5431, lng: 114.0579, country: "China" },
  "guangzhou": { lat: 23.1291, lng: 113.2644, country: "China" },
  "hong kong": { lat: 22.3193, lng: 114.1694, country: "Hong Kong" },
  "hangzhou": { lat: 30.2741, lng: 120.1551, country: "China" },
  "chengdu": { lat: 30.5728, lng: 104.0668, country: "China" },
  "xian": { lat: 34.3416, lng: 108.9398, country: "China" },
  "xi'an": { lat: 34.3416, lng: 108.9398, country: "China" },
  "nanjing": { lat: 32.0603, lng: 118.7969, country: "China" },
  "wuhan": { lat: 30.5928, lng: 114.3055, country: "China" },
  "tianjin": { lat: 39.3434, lng: 117.3616, country: "China" },
  "suzhou": { lat: 31.299, lng: 120.5853, country: "China" },
  "taipei": { lat: 25.033, lng: 121.5654, country: "Taiwan" },
  "kaohsiung": { lat: 22.6273, lng: 120.3014, country: "Taiwan" },
  "macau": { lat: 22.1987, lng: 113.5439, country: "Macau" },
  "macao": { lat: 22.1987, lng: 113.5439, country: "Macau" },
  // Asia - Southeast
  "singapore": { lat: 1.3521, lng: 103.8198, country: "Singapore" },
  "kuala lumpur": { lat: 3.139, lng: 101.6869, country: "Malaysia" },
  "penang": { lat: 5.4141, lng: 100.3288, country: "Malaysia" },
  "jakarta": { lat: -6.2088, lng: 106.8456, country: "Indonesia" },
  "bali": { lat: -8.3405, lng: 115.092, country: "Indonesia" },
  "denpasar": { lat: -8.6705, lng: 115.2126, country: "Indonesia" },
  "bandung": { lat: -6.9175, lng: 107.6191, country: "Indonesia" },
  "surabaya": { lat: -7.2575, lng: 112.7521, country: "Indonesia" },
  "bangkok": { lat: 13.7563, lng: 100.5018, country: "Thailand" },
  "chiang mai": { lat: 18.7883, lng: 98.9853, country: "Thailand" },
  "phuket": { lat: 7.8804, lng: 98.3923, country: "Thailand" },
  "ho chi minh city": { lat: 10.8231, lng: 106.6297, country: "Vietnam" },
  "saigon": { lat: 10.8231, lng: 106.6297, country: "Vietnam" },
  "hanoi": { lat: 21.0278, lng: 105.8342, country: "Vietnam" },
  "da nang": { lat: 16.0544, lng: 108.2022, country: "Vietnam" },
  "manila": { lat: 14.5995, lng: 120.9842, country: "Philippines" },
  "cebu": { lat: 10.3157, lng: 123.8854, country: "Philippines" },
  "phnom penh": { lat: 11.5564, lng: 104.9282, country: "Cambodia" },
  "yangon": { lat: 16.8661, lng: 96.1951, country: "Myanmar" },
  // Asia - South
  "mumbai": { lat: 19.076, lng: 72.8777, country: "India" },
  "bombay": { lat: 19.076, lng: 72.8777, country: "India" },
  "delhi": { lat: 28.7041, lng: 77.1025, country: "India" },
  "new delhi": { lat: 28.6139, lng: 77.209, country: "India" },
  "bangalore": { lat: 12.9716, lng: 77.5946, country: "India" },
  "bengaluru": { lat: 12.9716, lng: 77.5946, country: "India" },
  "hyderabad": { lat: 17.385, lng: 78.4867, country: "India" },
  "chennai": { lat: 13.0827, lng: 80.2707, country: "India" },
  "madras": { lat: 13.0827, lng: 80.2707, country: "India" },
  "pune": { lat: 18.5204, lng: 73.8567, country: "India" },
  "kolkata": { lat: 22.5726, lng: 88.3639, country: "India" },
  "calcutta": { lat: 22.5726, lng: 88.3639, country: "India" },
  "ahmedabad": { lat: 23.0225, lng: 72.5714, country: "India" },
  "jaipur": { lat: 26.9124, lng: 75.7873, country: "India" },
  "gurgaon": { lat: 28.4595, lng: 77.0266, country: "India" },
  "gurugram": { lat: 28.4595, lng: 77.0266, country: "India" },
  "noida": { lat: 28.5355, lng: 77.391, country: "India" },
  "kochi": { lat: 9.9312, lng: 76.2673, country: "India" },
  "goa": { lat: 15.2993, lng: 74.124, country: "India" },
  "karachi": { lat: 24.8607, lng: 67.0011, country: "Pakistan" },
  "lahore": { lat: 31.5497, lng: 74.3436, country: "Pakistan" },
  "islamabad": { lat: 33.6844, lng: 73.0479, country: "Pakistan" },
  "dhaka": { lat: 23.8103, lng: 90.4125, country: "Bangladesh" },
  "colombo": { lat: 6.9271, lng: 79.8612, country: "Sri Lanka" },
  "kathmandu": { lat: 27.7172, lng: 85.324, country: "Nepal" },
  // Africa
  "cape town": { lat: -33.9249, lng: 18.4241, country: "South Africa" },
  "johannesburg": { lat: -26.2041, lng: 28.0473, country: "South Africa" },
  "pretoria": { lat: -25.7479, lng: 28.2293, country: "South Africa" },
  "durban": { lat: -29.8587, lng: 31.0218, country: "South Africa" },
  "lagos": { lat: 6.5244, lng: 3.3792, country: "Nigeria" },
  "abuja": { lat: 9.0765, lng: 7.3986, country: "Nigeria" },
  "nairobi": { lat: -1.2921, lng: 36.8219, country: "Kenya" },
  "accra": { lat: 5.6037, lng: -0.187, country: "Ghana" },
  "casablanca": { lat: 33.5731, lng: -7.5898, country: "Morocco" },
  "marrakech": { lat: 31.6295, lng: -7.9811, country: "Morocco" },
  "tunis": { lat: 36.8065, lng: 10.1815, country: "Tunisia" },
  "algiers": { lat: 36.7538, lng: 3.0588, country: "Algeria" },
  "addis ababa": { lat: 8.9806, lng: 38.7578, country: "Ethiopia" },
  "dar es salaam": { lat: -6.7924, lng: 39.2083, country: "Tanzania" },
  "kampala": { lat: 0.3476, lng: 32.5825, country: "Uganda" },
  "kigali": { lat: -1.9403, lng: 29.8739, country: "Rwanda" },
  "mauritius": { lat: -20.3484, lng: 57.5522, country: "Mauritius" },
  "port louis": { lat: -20.1609, lng: 57.5012, country: "Mauritius" },
  // Oceania
  "sydney": { lat: -33.8688, lng: 151.2093, country: "Australia" },
  "melbourne": { lat: -37.8136, lng: 144.9631, country: "Australia" },
  "brisbane": { lat: -27.4698, lng: 153.0251, country: "Australia" },
  "perth": { lat: -31.9505, lng: 115.8605, country: "Australia" },
  "adelaide": { lat: -34.9285, lng: 138.6007, country: "Australia" },
  "canberra": { lat: -35.2809, lng: 149.13, country: "Australia" },
  "gold coast": { lat: -28.0167, lng: 153.4, country: "Australia" },
  "auckland": { lat: -36.8509, lng: 174.7645, country: "New Zealand" },
  "wellington": { lat: -41.2865, lng: 174.7762, country: "New Zealand" },
  "christchurch": { lat: -43.5321, lng: 172.6362, country: "New Zealand" },
  // Online/Virtual
  "online": { lat: 0, lng: 0, country: "Virtual", virtual: true },
  "virtual": { lat: 0, lng: 0, country: "Virtual", virtual: true },
  "hybrid": { lat: 0, lng: 0, country: "Virtual", virtual: true }
};

// server/worldmonitor/research/v1/list-tech-events.ts
var REDIS_CACHE_KEY8 = "research:tech-events:v1";
var REDIS_CACHE_TTL8 = 21600;
var ICS_URL = "https://www.techmeme.com/newsy_events.ics";
var DEV_EVENTS_RSS = "https://dev.events/rss.xml";
var CURATED_EVENTS = [
  {
    id: "step-dubai-2026",
    title: "STEP Dubai 2026",
    type: "conference",
    location: "Dubai Internet City, Dubai",
    coords: { lat: 25.0956, lng: 55.1548, country: "UAE", original: "Dubai Internet City, Dubai", virtual: false },
    startDate: "2026-02-11",
    endDate: "2026-02-12",
    url: "https://dubai.stepconference.com",
    source: "curated",
    description: "Intelligence Everywhere: The AI Economy - 8,000+ attendees, 400+ startups"
  },
  {
    id: "gitex-global-2026",
    title: "GITEX Global 2026",
    type: "conference",
    location: "Dubai World Trade Centre, Dubai",
    coords: { lat: 25.2285, lng: 55.2867, country: "UAE", original: "Dubai World Trade Centre, Dubai", virtual: false },
    startDate: "2026-12-07",
    endDate: "2026-12-11",
    url: "https://www.gitex.com",
    source: "curated",
    description: "World's largest tech & startup show"
  },
  {
    id: "token2049-dubai-2026",
    title: "TOKEN2049 Dubai 2026",
    type: "conference",
    location: "Dubai, UAE",
    coords: { lat: 25.2048, lng: 55.2708, country: "UAE", original: "Dubai, UAE", virtual: false },
    startDate: "2026-04-29",
    endDate: "2026-04-30",
    url: "https://www.token2049.com",
    source: "curated",
    description: "Premier crypto event in Dubai"
  },
  {
    id: "collision-2026",
    title: "Collision 2026",
    type: "conference",
    location: "Toronto, Canada",
    coords: { lat: 43.6532, lng: -79.3832, country: "Canada", original: "Toronto, Canada", virtual: false },
    startDate: "2026-06-22",
    endDate: "2026-06-25",
    url: "https://collisionconf.com",
    source: "curated",
    description: "North America's fastest growing tech conference"
  },
  {
    id: "web-summit-2026",
    title: "Web Summit 2026",
    type: "conference",
    location: "Lisbon, Portugal",
    coords: { lat: 38.7223, lng: -9.1393, country: "Portugal", original: "Lisbon, Portugal", virtual: false },
    startDate: "2026-11-02",
    endDate: "2026-11-05",
    url: "https://websummit.com",
    source: "curated",
    description: "The world's premier tech conference"
  }
];
function normalizeLocation(location) {
  if (!location) return null;
  let normalized = location.toLowerCase().trim();
  normalized = normalized.replace(/^hybrid:\s*/i, "");
  normalized = normalized.replace(/,\s*(usa|us|uk|canada)$/i, "");
  if (CITY_COORDS[normalized]) {
    const c = CITY_COORDS[normalized];
    return { lat: c.lat, lng: c.lng, country: c.country, original: location, virtual: c.virtual ?? false };
  }
  const parts = normalized.split(",");
  if (parts.length > 1) {
    const city = parts[0].trim();
    if (CITY_COORDS[city]) {
      const c = CITY_COORDS[city];
      return { lat: c.lat, lng: c.lng, country: c.country, original: location, virtual: c.virtual ?? false };
    }
  }
  for (const [key, coords] of Object.entries(CITY_COORDS)) {
    if (normalized.includes(key) || key.includes(normalized)) {
      return { lat: coords.lat, lng: coords.lng, country: coords.country, original: location, virtual: coords.virtual ?? false };
    }
  }
  return null;
}
function parseICS(icsText) {
  const events = [];
  const eventBlocks = icsText.split("BEGIN:VEVENT").slice(1);
  for (const block of eventBlocks) {
    const summaryMatch = block.match(/SUMMARY:(.+)/);
    const locationMatch = block.match(/LOCATION:(.+)/);
    const dtstartMatch = block.match(/DTSTART;VALUE=DATE:(\d+)/);
    const dtendMatch = block.match(/DTEND;VALUE=DATE:(\d+)/);
    const urlMatch = block.match(/URL:(.+)/);
    const uidMatch = block.match(/UID:(.+)/);
    if (summaryMatch && dtstartMatch) {
      const summary = summaryMatch[1].trim();
      const location = locationMatch ? locationMatch[1].trim() : "";
      const startDate = dtstartMatch[1];
      const endDate = dtendMatch ? dtendMatch[1] : startDate;
      const url = urlMatch ? urlMatch[1].trim() : "";
      const uid = uidMatch ? uidMatch[1].trim() : "";
      let type = "other";
      if (summary.startsWith("Earnings:")) type = "earnings";
      else if (summary.startsWith("IPO")) type = "ipo";
      else if (location) type = "conference";
      const coords = normalizeLocation(location || null);
      events.push({
        id: uid,
        title: summary,
        type,
        location,
        coords: coords ?? void 0,
        startDate: `${startDate.slice(0, 4)}-${startDate.slice(4, 6)}-${startDate.slice(6, 8)}`,
        endDate: `${endDate.slice(0, 4)}-${endDate.slice(4, 6)}-${endDate.slice(6, 8)}`,
        url,
        source: "techmeme",
        description: ""
      });
    }
  }
  return events.sort((a, b) => a.startDate.localeCompare(b.startDate));
}
function parseDevEventsRSS(rssText) {
  const events = [];
  const itemMatches = rssText.matchAll(/<item>([\s\S]*?)<\/item>/g);
  for (const match of itemMatches) {
    const item = match[1];
    const titleMatch = item.match(/<title><!\[CDATA\[(.*?)\]\]><\/title>|<title>(.*?)<\/title>/);
    const linkMatch = item.match(/<link>(.*?)<\/link>/);
    const descMatch = item.match(/<description><!\[CDATA\[(.*?)\]\]><\/description>|<description>(.*?)<\/description>/s);
    const guidMatch = item.match(/<guid[^>]*>(.*?)<\/guid>/);
    const title = titleMatch ? titleMatch[1] ?? titleMatch[2] : null;
    const link = linkMatch ? linkMatch[1] ?? "" : "";
    const description = descMatch ? descMatch[1] ?? descMatch[2] ?? "" : "";
    const guid = guidMatch ? guidMatch[1] ?? "" : "";
    if (!title) continue;
    const dateMatch = description.match(/on\s+(\w+\s+\d{1,2},?\s+\d{4})/i);
    let startDate = null;
    if (dateMatch) {
      const parsed = new Date(dateMatch[1]);
      if (!isNaN(parsed.getTime())) {
        startDate = parsed.toISOString().split("T")[0];
      }
    }
    let location = null;
    const locationMatch = description.match(/(?:in|at)\s+([A-Za-z\s]+,\s*[A-Za-z\s]+)(?:\.|$)/i) || description.match(/Location:\s*([^<\n]+)/i);
    if (locationMatch) {
      location = locationMatch[1].trim();
    }
    if (description.toLowerCase().includes("online")) {
      location = "Online";
    }
    if (!startDate) continue;
    const eventDate = new Date(startDate);
    const now = /* @__PURE__ */ new Date();
    now.setHours(0, 0, 0, 0);
    if (eventDate < now) continue;
    const coords = location && location !== "Online" ? normalizeLocation(location) : null;
    events.push({
      id: guid || `dev-events-${title.slice(0, 20)}`,
      title,
      type: "conference",
      location: location || "",
      coords: coords ?? (location === "Online" ? { lat: 0, lng: 0, country: "Virtual", original: "Online", virtual: true } : void 0),
      startDate,
      endDate: startDate,
      // RSS doesn't have end date
      url: link,
      source: "dev.events",
      description: ""
    });
  }
  return events;
}
async function fetchTechEvents(req) {
  const { type, mappable, limit, days } = req;
  const [icsResponse, rssResponse] = await Promise.allSettled([
    fetch(ICS_URL, {
      headers: { "User-Agent": CHROME_UA }
    }),
    fetch(DEV_EVENTS_RSS, {
      headers: { "User-Agent": CHROME_UA }
    })
  ]);
  let events = [];
  if (icsResponse.status === "fulfilled" && icsResponse.value.ok) {
    const icsText = await icsResponse.value.text();
    events.push(...parseICS(icsText));
  } else {
    console.warn("Failed to fetch Techmeme ICS");
  }
  if (rssResponse.status === "fulfilled" && rssResponse.value.ok) {
    const rssText = await rssResponse.value.text();
    const devEvents = parseDevEventsRSS(rssText);
    events.push(...devEvents);
  } else {
    console.warn("Failed to fetch dev.events RSS");
  }
  const now = /* @__PURE__ */ new Date();
  now.setHours(0, 0, 0, 0);
  for (const curated of CURATED_EVENTS) {
    const eventDate = new Date(curated.startDate);
    if (eventDate >= now) {
      events.push(curated);
    }
  }
  const seen = /* @__PURE__ */ new Set();
  events = events.filter((e) => {
    const year = e.startDate.slice(0, 4);
    const key = e.title.toLowerCase().replace(/[^a-z0-9]/g, "").slice(0, 30) + year;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
  events.sort((a, b) => a.startDate.localeCompare(b.startDate));
  if (type && type !== "all") {
    events = events.filter((e) => e.type === type);
  }
  if (mappable) {
    events = events.filter((e) => e.coords && !e.coords.virtual);
  }
  if (days > 0) {
    const cutoff = /* @__PURE__ */ new Date();
    cutoff.setDate(cutoff.getDate() + days);
    events = events.filter((e) => new Date(e.startDate) <= cutoff);
  }
  if (limit > 0) {
    events = events.slice(0, limit);
  }
  const conferences = events.filter((e) => e.type === "conference");
  const mappableCount = conferences.filter((e) => e.coords && !e.coords.virtual).length;
  return {
    success: true,
    count: events.length,
    conferenceCount: conferences.length,
    mappableCount,
    lastUpdated: (/* @__PURE__ */ new Date()).toISOString(),
    events,
    error: ""
  };
}
function applyLimit(res, limit) {
  const events = res.events.slice(0, limit);
  const conferences = events.filter((e) => e.type === "conference");
  const mappableCount = conferences.filter((e) => e.coords && !e.coords.virtual).length;
  return { ...res, events, count: events.length, conferenceCount: conferences.length, mappableCount };
}
async function listTechEvents(_ctx, req) {
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY8}:${req.type || "all"}:${req.mappable ? 1 : 0}:${req.days || 0}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL8, async () => {
      const fetched = await fetchTechEvents({ ...req, limit: 0 });
      return fetched.events.length > 0 ? fetched : null;
    });
    if (!result) {
      return { success: true, count: 0, conferenceCount: 0, mappableCount: 0, lastUpdated: (/* @__PURE__ */ new Date()).toISOString(), events: [], error: "" };
    }
    if (req.limit > 0 && result.events.length > req.limit) {
      return applyLimit(result, req.limit);
    }
    return result;
  } catch (error) {
    return {
      success: false,
      count: 0,
      conferenceCount: 0,
      mappableCount: 0,
      lastUpdated: (/* @__PURE__ */ new Date()).toISOString(),
      events: [],
      error: error instanceof Error ? error.message : String(error)
    };
  }
}

// server/worldmonitor/research/v1/handler.ts
var researchHandler = {
  listArxivPapers,
  listTrendingRepos,
  listHackernewsItems,
  listTechEvents
};

// src/generated/server/worldmonitor/unrest/v1/service_server.ts
var ValidationError8 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createUnrestServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/unrest/v1/list-unrest-events",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            start: Number(params.get("start") ?? "0"),
            end: Number(params.get("end") ?? "0"),
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            country: params.get("country") ?? "",
            minSeverity: params.get("min_severity") ?? "SEVERITY_LEVEL_UNSPECIFIED",
            neLat: Number(params.get("ne_lat") ?? "0"),
            neLon: Number(params.get("ne_lon") ?? "0"),
            swLat: Number(params.get("sw_lat") ?? "0"),
            swLon: Number(params.get("sw_lon") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listUnrestEvents", body);
            if (bodyViolations) {
              throw new ValidationError8(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listUnrestEvents(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError8) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/unrest/v1/_shared.ts
var GDELT_GEO_URL = "https://api.gdeltproject.org/api/v2/geo/geo";
function mapAcledEventType(eventType, subEventType) {
  const lower = (eventType + " " + subEventType).toLowerCase();
  if (lower.includes("riot") || lower.includes("mob violence"))
    return "UNREST_EVENT_TYPE_RIOT";
  if (lower.includes("strike"))
    return "UNREST_EVENT_TYPE_STRIKE";
  if (lower.includes("demonstration"))
    return "UNREST_EVENT_TYPE_DEMONSTRATION";
  if (lower.includes("protest"))
    return "UNREST_EVENT_TYPE_PROTEST";
  return "UNREST_EVENT_TYPE_CIVIL_UNREST";
}
function classifySeverity2(fatalities, eventType) {
  if (fatalities > 0 || eventType.toLowerCase().includes("riot"))
    return "SEVERITY_LEVEL_HIGH";
  if (eventType.toLowerCase().includes("protest"))
    return "SEVERITY_LEVEL_MEDIUM";
  return "SEVERITY_LEVEL_LOW";
}
function classifyGdeltSeverity(count, name) {
  const lowerName = name.toLowerCase();
  if (count > 100 || lowerName.includes("riot") || lowerName.includes("clash"))
    return "SEVERITY_LEVEL_HIGH";
  if (count < 25)
    return "SEVERITY_LEVEL_LOW";
  return "SEVERITY_LEVEL_MEDIUM";
}
function classifyGdeltEventType(name) {
  const lowerName = name.toLowerCase();
  if (lowerName.includes("riot")) return "UNREST_EVENT_TYPE_RIOT";
  if (lowerName.includes("strike")) return "UNREST_EVENT_TYPE_STRIKE";
  if (lowerName.includes("demonstration")) return "UNREST_EVENT_TYPE_DEMONSTRATION";
  return "UNREST_EVENT_TYPE_PROTEST";
}
function deduplicateEvents(events) {
  const unique = /* @__PURE__ */ new Map();
  for (const event of events) {
    const lat = event.location?.latitude ?? 0;
    const lon = event.location?.longitude ?? 0;
    const latKey = Math.round(lat * 10) / 10;
    const lonKey = Math.round(lon * 10) / 10;
    const dateKey = new Date(event.occurredAt).toISOString().split("T")[0];
    const key = `${latKey}:${lonKey}:${dateKey}`;
    const existing = unique.get(key);
    if (!existing) {
      unique.set(key, event);
    } else {
      if (event.sourceType === "UNREST_SOURCE_TYPE_ACLED" && existing.sourceType !== "UNREST_SOURCE_TYPE_ACLED") {
        event.sources = [.../* @__PURE__ */ new Set([...event.sources, ...existing.sources])];
        unique.set(key, event);
      } else if (existing.sourceType === "UNREST_SOURCE_TYPE_ACLED") {
        existing.sources = [.../* @__PURE__ */ new Set([...existing.sources, ...event.sources])];
      } else {
        existing.sources = [.../* @__PURE__ */ new Set([...existing.sources, ...event.sources])];
        if (existing.sources.length >= 2) {
          existing.confidence = "CONFIDENCE_LEVEL_HIGH";
        }
      }
    }
  }
  return Array.from(unique.values());
}
function sortBySeverityAndRecency(events) {
  const severityOrder = {
    SEVERITY_LEVEL_HIGH: 0,
    SEVERITY_LEVEL_MEDIUM: 1,
    SEVERITY_LEVEL_LOW: 2,
    SEVERITY_LEVEL_UNSPECIFIED: 3
  };
  return events.sort((a, b) => {
    const sevDiff = (severityOrder[a.severity] ?? 3) - (severityOrder[b.severity] ?? 3);
    if (sevDiff !== 0) return sevDiff;
    return b.occurredAt - a.occurredAt;
  });
}

// server/_shared/acled.ts
var ACLED_API_URL = "https://acleddata.com/api/acled/read";
var ACLED_CACHE_TTL = 900;
var ACLED_TIMEOUT_MS = 15e3;
async function fetchAcledCached(opts) {
  const token = process.env.ACLED_ACCESS_TOKEN;
  if (!token) return [];
  const cacheKey2 = `acled:shared:${opts.eventTypes}:${opts.startDate}:${opts.endDate}:${opts.country || "all"}:${opts.limit || 500}`;
  const result = await cachedFetchJson(cacheKey2, ACLED_CACHE_TTL, async () => {
    const params = new URLSearchParams({
      event_type: opts.eventTypes,
      event_date: `${opts.startDate}|${opts.endDate}`,
      event_date_where: "BETWEEN",
      limit: String(opts.limit || 500),
      _format: "json"
    });
    if (opts.country) params.set("country", opts.country);
    const resp = await fetch(`${ACLED_API_URL}?${params}`, {
      headers: {
        Accept: "application/json",
        Authorization: `Bearer ${token}`,
        "User-Agent": CHROME_UA
      },
      signal: AbortSignal.timeout(ACLED_TIMEOUT_MS)
    });
    if (!resp.ok) throw new Error(`ACLED API error: ${resp.status}`);
    const data = await resp.json();
    if (data.message || data.error) throw new Error(data.message || data.error || "ACLED API error");
    const events = data.data || [];
    return events.length > 0 ? events : null;
  });
  return result || [];
}

// server/worldmonitor/unrest/v1/list-unrest-events.ts
var REDIS_CACHE_KEY9 = "unrest:events:v1";
var REDIS_CACHE_TTL9 = 900;
async function fetchAcledProtests(req) {
  try {
    const now = Date.now();
    const startMs = req.start ?? now - 30 * 24 * 60 * 60 * 1e3;
    const endMs = req.end ?? now;
    const startDate = new Date(startMs).toISOString().split("T")[0];
    const endDate = new Date(endMs).toISOString().split("T")[0];
    const rawEvents = await fetchAcledCached({
      eventTypes: "Protests",
      startDate,
      endDate,
      country: req.country || void 0
    });
    return rawEvents.filter((e) => {
      const lat = parseFloat(e.latitude || "");
      const lon = parseFloat(e.longitude || "");
      return Number.isFinite(lat) && Number.isFinite(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
    }).map((e) => {
      const fatalities = parseInt(e.fatalities || "", 10) || 0;
      return {
        id: `acled-${e.event_id_cnty}`,
        title: e.notes?.slice(0, 200) || `${e.sub_event_type} in ${e.location}`,
        summary: typeof e.notes === "string" ? e.notes.substring(0, 500) : "",
        eventType: mapAcledEventType(e.event_type || "", e.sub_event_type || ""),
        city: e.location || "",
        country: e.country || "",
        region: e.admin1 || "",
        location: {
          latitude: parseFloat(e.latitude || "0"),
          longitude: parseFloat(e.longitude || "0")
        },
        occurredAt: new Date(e.event_date || "").getTime(),
        severity: classifySeverity2(fatalities, e.event_type || ""),
        fatalities,
        sources: [e.source].filter(Boolean),
        sourceType: "UNREST_SOURCE_TYPE_ACLED",
        tags: e.tags?.split(";").map((t) => t.trim()).filter(Boolean) ?? [],
        actors: [e.actor1, e.actor2].filter(Boolean),
        confidence: "CONFIDENCE_LEVEL_HIGH"
      };
    });
  } catch {
    return [];
  }
}
async function fetchGdeltEvents() {
  try {
    const params = new URLSearchParams({
      query: "protest",
      format: "geojson",
      maxrecords: "250",
      timespan: "7d"
    });
    const response = await fetch(`${GDELT_GEO_URL}?${params}`, {
      headers: { Accept: "application/json", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(1e4)
    });
    if (!response.ok) return [];
    const data = await response.json();
    const features = data?.features || [];
    const seenLocations = /* @__PURE__ */ new Set();
    const events = [];
    for (const feature of features) {
      const name = feature.properties?.name || "";
      if (!name || seenLocations.has(name)) continue;
      const count = feature.properties?.count || 1;
      if (count < 5) continue;
      const coords = feature.geometry?.coordinates;
      if (!Array.isArray(coords) || coords.length < 2) continue;
      const [lon, lat] = coords;
      if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180)
        continue;
      seenLocations.add(name);
      const country = name.split(",").pop()?.trim() || name;
      events.push({
        id: `gdelt-${lat.toFixed(2)}-${lon.toFixed(2)}-${Date.now()}`,
        title: `${name} (${count} reports)`,
        summary: "",
        eventType: classifyGdeltEventType(name),
        city: name.split(",")[0]?.trim() || "",
        country,
        region: "",
        location: { latitude: lat, longitude: lon },
        occurredAt: Date.now(),
        severity: classifyGdeltSeverity(count, name),
        fatalities: 0,
        sources: ["GDELT"],
        sourceType: "UNREST_SOURCE_TYPE_GDELT",
        tags: [],
        actors: [],
        confidence: count > 20 ? "CONFIDENCE_LEVEL_HIGH" : "CONFIDENCE_LEVEL_MEDIUM"
      });
    }
    return events;
  } catch {
    return [];
  }
}
async function listUnrestEvents(_ctx, req) {
  try {
    const startBucket = req.start > 0 ? new Date(req.start).toISOString().slice(0, 10) : "default";
    const endBucket = req.end > 0 ? new Date(req.end).toISOString().slice(0, 10) : "default";
    const cacheKey2 = `${REDIS_CACHE_KEY9}:${req.country || "all"}:${startBucket}:${endBucket}`;
    const result = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL9,
      async () => {
        const [acledEvents, gdeltEvents] = await Promise.all([
          fetchAcledProtests(req),
          fetchGdeltEvents()
        ]);
        const merged = deduplicateEvents([...acledEvents, ...gdeltEvents]);
        const sorted = sortBySeverityAndRecency(merged);
        return sorted.length > 0 ? { events: sorted, clusters: [], pagination: void 0 } : null;
      }
    );
    return result || { events: [], clusters: [], pagination: void 0 };
  } catch {
    return { events: [], clusters: [], pagination: void 0 };
  }
}

// server/worldmonitor/unrest/v1/handler.ts
var unrestHandler = {
  listUnrestEvents
};

// src/generated/server/worldmonitor/conflict/v1/service_server.ts
var ValidationError9 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createConflictServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/conflict/v1/list-acled-events",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            start: Number(params.get("start") ?? "0"),
            end: Number(params.get("end") ?? "0"),
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            country: params.get("country") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listAcledEvents", body);
            if (bodyViolations) {
              throw new ValidationError9(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listAcledEvents(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError9) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/conflict/v1/list-ucdp-events",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            start: Number(params.get("start") ?? "0"),
            end: Number(params.get("end") ?? "0"),
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            country: params.get("country") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listUcdpEvents", body);
            if (bodyViolations) {
              throw new ValidationError9(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listUcdpEvents(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError9) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/conflict/v1/get-humanitarian-summary",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            countryCode: params.get("country_code") ?? ""
          };
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getHumanitarianSummary(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError9) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/conflict/v1/list-iran-events",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listIranEvents(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError9) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/conflict/v1/list-acled-events.ts
var REDIS_CACHE_KEY10 = "conflict:acled:v1";
var REDIS_CACHE_TTL10 = 900;
var fallbackAcledCache = /* @__PURE__ */ new Map();
async function fetchAcledConflicts(req) {
  try {
    const now = Date.now();
    const startMs = req.start ?? now - 30 * 24 * 60 * 60 * 1e3;
    const endMs = req.end ?? now;
    const startDate = new Date(startMs).toISOString().split("T")[0];
    const endDate = new Date(endMs).toISOString().split("T")[0];
    const rawEvents = await fetchAcledCached({
      eventTypes: "Battles|Explosions/Remote violence|Violence against civilians",
      startDate,
      endDate,
      country: req.country || void 0
    });
    return rawEvents.filter((e) => {
      const lat = parseFloat(e.latitude || "");
      const lon = parseFloat(e.longitude || "");
      return Number.isFinite(lat) && Number.isFinite(lon) && lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
    }).map((e) => ({
      id: `acled-${e.event_id_cnty}`,
      eventType: e.event_type || "",
      country: e.country || "",
      location: {
        latitude: parseFloat(e.latitude || "0"),
        longitude: parseFloat(e.longitude || "0")
      },
      occurredAt: new Date(e.event_date || "").getTime(),
      fatalities: parseInt(e.fatalities || "", 10) || 0,
      actors: [e.actor1, e.actor2].filter(Boolean),
      source: e.source || "",
      admin1: e.admin1 || ""
    }));
  } catch {
    return [];
  }
}
async function listAcledEvents(_ctx, req) {
  const cacheKey2 = `${REDIS_CACHE_KEY10}:${req.country || "all"}:${req.start || 0}:${req.end || 0}`;
  try {
    const result = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL10,
      async () => {
        const events = await fetchAcledConflicts(req);
        return events.length > 0 ? { events, pagination: void 0 } : null;
      }
    );
    if (result) {
      if (fallbackAcledCache.size > 50) fallbackAcledCache.clear();
      fallbackAcledCache.set(cacheKey2, { data: result, ts: Date.now() });
    }
    return result || fallbackAcledCache.get(cacheKey2)?.data || { events: [], pagination: void 0 };
  } catch {
    return fallbackAcledCache.get(cacheKey2)?.data || { events: [], pagination: void 0 };
  }
}

// server/worldmonitor/conflict/v1/list-ucdp-events.ts
var UCDP_PAGE_SIZE = 1e3;
var MAX_PAGES = 12;
var TRAILING_WINDOW_MS = 365 * 24 * 60 * 60 * 1e3;
var CACHE_KEY2 = "ucdp:gedevents:sebuf:v1";
var CACHE_TTL_FULL = 6 * 60 * 60;
var CACHE_TTL_PARTIAL = 10 * 60;
var fallbackCache = {
  data: null,
  timestamp: 0,
  ttlMs: CACHE_TTL_FULL * 1e3
};
var VIOLENCE_TYPE_MAP = {
  1: "UCDP_VIOLENCE_TYPE_STATE_BASED",
  2: "UCDP_VIOLENCE_TYPE_NON_STATE",
  3: "UCDP_VIOLENCE_TYPE_ONE_SIDED"
};
function parseDateMs(value) {
  if (!value) return NaN;
  return Date.parse(String(value));
}
function getMaxDateMs(events) {
  let maxMs = NaN;
  for (const event of events) {
    const ms = parseDateMs(event?.date_start);
    if (!Number.isFinite(ms)) continue;
    if (!Number.isFinite(maxMs) || ms > maxMs) {
      maxMs = ms;
    }
  }
  return maxMs;
}
function buildVersionCandidates() {
  const year = (/* @__PURE__ */ new Date()).getFullYear() - 2e3;
  return Array.from(/* @__PURE__ */ new Set([`${year}.1`, `${year - 1}.1`, "25.1", "24.1"]));
}
var lastFailureTimestamp = 0;
var NEGATIVE_CACHE_MS = 60 * 1e3;
var discoveredVersion = null;
var discoveredVersionTimestamp = 0;
var VERSION_CACHE_MS = 60 * 60 * 1e3;
async function fetchGedPage(version, page) {
  const response = await fetch(
    `https://ucdpapi.pcr.uu.se/api/gedevents/${version}?pagesize=${UCDP_PAGE_SIZE}&page=${page}`,
    {
      headers: { Accept: "application/json", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(15e3)
    }
  );
  if (!response.ok) {
    throw new Error(`UCDP GED API error (${version}, page ${page}): ${response.status}`);
  }
  return response.json();
}
async function discoverGedVersion() {
  if (discoveredVersion && Date.now() - discoveredVersionTimestamp < VERSION_CACHE_MS) {
    const page0 = await fetchGedPage(discoveredVersion, 0);
    if (Array.isArray(page0?.Result)) {
      return { version: discoveredVersion, page0 };
    }
    discoveredVersion = null;
  }
  const candidates = buildVersionCandidates();
  const results = await Promise.allSettled(
    candidates.map(async (version) => {
      const page0 = await fetchGedPage(version, 0);
      if (!Array.isArray(page0?.Result)) throw new Error("No results");
      return { version, page0 };
    })
  );
  for (const result of results) {
    if (result.status === "fulfilled") {
      discoveredVersion = result.value.version;
      discoveredVersionTimestamp = Date.now();
      return result.value;
    }
  }
  throw new Error("No valid UCDP GED version found");
}
async function fetchUcdpGedEvents() {
  if (lastFailureTimestamp && Date.now() - lastFailureTimestamp < NEGATIVE_CACHE_MS) {
    return null;
  }
  try {
    const { version, page0 } = await discoverGedVersion();
    const totalPages = Math.max(1, Number(page0?.TotalPages) || 1);
    const newestPage = totalPages - 1;
    const FAILED = /* @__PURE__ */ Symbol("failed");
    const pagesToFetch = [];
    for (let offset = 0; offset < MAX_PAGES && newestPage - offset >= 0; offset++) {
      const page = newestPage - offset;
      if (page === 0) {
        pagesToFetch.push(Promise.resolve(page0));
      } else {
        pagesToFetch.push(fetchGedPage(version, page).catch(() => FAILED));
      }
    }
    const pageResults = await Promise.all(pagesToFetch);
    const allEvents = [];
    let latestDatasetMs = NaN;
    let failedPages = 0;
    for (const rawData of pageResults) {
      if (rawData === FAILED) {
        failedPages++;
        continue;
      }
      const events = Array.isArray(rawData?.Result) ? rawData.Result : [];
      allEvents.push(...events);
      const pageMaxMs = getMaxDateMs(events);
      if (!Number.isFinite(latestDatasetMs) && Number.isFinite(pageMaxMs)) {
        latestDatasetMs = pageMaxMs;
      }
    }
    const isPartial = failedPages > 0;
    const filtered = allEvents.filter((event) => {
      if (!Number.isFinite(latestDatasetMs)) return true;
      const eventMs = parseDateMs(event?.date_start);
      if (!Number.isFinite(eventMs)) return false;
      return eventMs >= latestDatasetMs - TRAILING_WINDOW_MS;
    });
    const mapped = filtered.map((e) => ({
      id: String(e.id || ""),
      dateStart: Date.parse(e.date_start) || 0,
      dateEnd: Date.parse(e.date_end) || 0,
      location: {
        latitude: Number(e.latitude) || 0,
        longitude: Number(e.longitude) || 0
      },
      country: e.country || "",
      sideA: (e.side_a || "").substring(0, 200),
      sideB: (e.side_b || "").substring(0, 200),
      deathsBest: Number(e.best) || 0,
      deathsLow: Number(e.low) || 0,
      deathsHigh: Number(e.high) || 0,
      violenceType: VIOLENCE_TYPE_MAP[e.type_of_violence] || "UCDP_VIOLENCE_TYPE_UNSPECIFIED",
      sourceOriginal: (e.source_original || "").substring(0, 300)
    }));
    mapped.sort((a, b) => b.dateStart - a.dateStart);
    lastFailureTimestamp = 0;
    if (mapped.length === 0) return null;
    if (isPartial) {
      fallbackCache = { data: mapped, timestamp: Date.now(), ttlMs: CACHE_TTL_PARTIAL * 1e3 };
      return null;
    }
    return mapped;
  } catch {
    lastFailureTimestamp = Date.now();
    return null;
  }
}
async function listUcdpEvents(_ctx, req) {
  try {
    const cached = await cachedFetchJson(CACHE_KEY2, CACHE_TTL_FULL, fetchUcdpGedEvents);
    if (cached && Array.isArray(cached) && cached.length > 0) {
      fallbackCache = { data: cached, timestamp: Date.now(), ttlMs: CACHE_TTL_FULL * 1e3 };
      let events = cached;
      if (req.country) events = events.filter((e) => e.country === req.country);
      return { events, pagination: void 0 };
    }
  } catch {
  }
  if (fallbackCache.data && Date.now() - fallbackCache.timestamp < fallbackCache.ttlMs) {
    let events = fallbackCache.data;
    if (req.country) events = events.filter((e) => e.country === req.country);
    return { events, pagination: void 0 };
  }
  fallbackCache = { data: null, timestamp: 0, ttlMs: CACHE_TTL_FULL * 1e3 };
  return { events: [], pagination: void 0 };
}

// server/worldmonitor/conflict/v1/get-humanitarian-summary.ts
var REDIS_CACHE_KEY11 = "conflict:humanitarian:v1";
var REDIS_CACHE_TTL11 = 21600;
var ISO2_TO_ISO3 = {
  US: "USA",
  RU: "RUS",
  CN: "CHN",
  UA: "UKR",
  IR: "IRN",
  IL: "ISR",
  TW: "TWN",
  KP: "PRK",
  SA: "SAU",
  TR: "TUR",
  PL: "POL",
  DE: "DEU",
  FR: "FRA",
  GB: "GBR",
  IN: "IND",
  PK: "PAK",
  SY: "SYR",
  YE: "YEM",
  MM: "MMR",
  VE: "VEN",
  AF: "AFG",
  SD: "SDN",
  SS: "SSD",
  SO: "SOM",
  CD: "COD",
  ET: "ETH",
  IQ: "IRQ",
  CO: "COL",
  NG: "NGA",
  PS: "PSE",
  BR: "BRA",
  AE: "ARE"
};
async function fetchHapiSummary(countryCode) {
  try {
    const appId = btoa("worldmonitor:monitor@worldmonitor.app");
    let url = `https://hapi.humdata.org/api/v2/coordination-context/conflict-events?output_format=json&limit=1000&offset=0&app_identifier=${appId}`;
    if (countryCode) {
      const iso3 = ISO2_TO_ISO3[countryCode.toUpperCase()];
      if (!iso3) return void 0;
      url += `&location_code=${iso3}`;
    }
    const response = await fetch(url, {
      headers: { Accept: "application/json", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(15e3)
    });
    if (!response.ok) return void 0;
    const rawData = await response.json();
    const records = rawData.data || [];
    const byCountry = {};
    for (const r of records) {
      const iso3 = r.location_code || "";
      if (!iso3) continue;
      const month = r.reference_period_start || "";
      const eventType = (r.event_type || "").toLowerCase();
      const events = r.events || 0;
      const fatalities = r.fatalities || 0;
      if (!byCountry[iso3]) {
        byCountry[iso3] = {
          iso3,
          locationName: r.location_name || "",
          month,
          eventsTotal: 0,
          eventsPoliticalViolence: 0,
          eventsCivilianTargeting: 0,
          eventsDemonstrations: 0,
          fatalitiesTotalPoliticalViolence: 0,
          fatalitiesTotalCivilianTargeting: 0
        };
      }
      const c = byCountry[iso3];
      if (month > c.month) {
        c.month = month;
        c.eventsTotal = 0;
        c.eventsPoliticalViolence = 0;
        c.eventsCivilianTargeting = 0;
        c.eventsDemonstrations = 0;
        c.fatalitiesTotalPoliticalViolence = 0;
        c.fatalitiesTotalCivilianTargeting = 0;
      }
      if (month === c.month) {
        c.eventsTotal += events;
        if (eventType.includes("political_violence")) {
          c.eventsPoliticalViolence += events;
          c.fatalitiesTotalPoliticalViolence += fatalities;
        }
        if (eventType.includes("civilian_targeting")) {
          c.eventsCivilianTargeting += events;
          c.fatalitiesTotalCivilianTargeting += fatalities;
        }
        if (eventType.includes("demonstration")) {
          c.eventsDemonstrations += events;
        }
      }
    }
    let entry;
    if (countryCode) {
      const iso3 = ISO2_TO_ISO3[countryCode.toUpperCase()];
      entry = iso3 ? byCountry[iso3] : void 0;
      if (!entry) return void 0;
    } else {
      entry = Object.values(byCountry)[0];
    }
    if (!entry) return void 0;
    return {
      countryCode: countryCode ? countryCode.toUpperCase() : "",
      countryName: entry.locationName,
      conflictEventsTotal: entry.eventsTotal,
      conflictPoliticalViolenceEvents: entry.eventsPoliticalViolence + entry.eventsCivilianTargeting,
      conflictFatalities: entry.fatalitiesTotalPoliticalViolence + entry.fatalitiesTotalCivilianTargeting,
      referencePeriod: entry.month,
      conflictDemonstrations: entry.eventsDemonstrations,
      updatedAt: Date.now()
    };
  } catch {
    return void 0;
  }
}
async function getHumanitarianSummary(_ctx, req) {
  if (!req.countryCode) return { summary: void 0 };
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY11}:${req.countryCode || "all"}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL11, async () => {
      const summary = await fetchHapiSummary(req.countryCode);
      return summary ? { summary } : null;
    });
    return result || { summary: void 0 };
  } catch {
    return { summary: void 0 };
  }
}

// server/worldmonitor/conflict/v1/list-iran-events.ts
var REDIS_KEY = "conflict:iran-events:v1";
async function listIranEvents(_ctx, _req) {
  try {
    const cached = await getCachedJson(REDIS_KEY);
    if (cached && typeof cached === "object" && "events" in cached) {
      return cached;
    }
    return { events: [], scrapedAt: 0 };
  } catch {
    return { events: [], scrapedAt: 0 };
  }
}

// server/worldmonitor/conflict/v1/handler.ts
var conflictHandler = {
  listAcledEvents,
  listUcdpEvents,
  getHumanitarianSummary,
  listIranEvents
};

// src/generated/server/worldmonitor/maritime/v1/service_server.ts
var ValidationError10 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createMaritimeServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/maritime/v1/get-vessel-snapshot",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            neLat: Number(params.get("ne_lat") ?? "0"),
            neLon: Number(params.get("ne_lon") ?? "0"),
            swLat: Number(params.get("sw_lat") ?? "0"),
            swLon: Number(params.get("sw_lon") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getVesselSnapshot", body);
            if (bodyViolations) {
              throw new ValidationError10(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getVesselSnapshot(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError10) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/maritime/v1/list-navigational-warnings",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            area: params.get("area") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listNavigationalWarnings", body);
            if (bodyViolations) {
              throw new ValidationError10(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listNavigationalWarnings(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError10) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/maritime/v1/get-vessel-snapshot.ts
function getRelayBaseUrl2() {
  const relayUrl = process.env.WS_RELAY_URL;
  if (!relayUrl) return null;
  return relayUrl.replace("wss://", "https://").replace("ws://", "http://").replace(/\/$/, "");
}
function getRelayRequestHeaders() {
  const headers = {
    Accept: "application/json",
    "User-Agent": CHROME_UA
  };
  const relaySecret = process.env.RELAY_SHARED_SECRET;
  if (relaySecret) {
    const relayHeader = (process.env.RELAY_AUTH_HEADER || "x-relay-key").toLowerCase();
    headers[relayHeader] = relaySecret;
    headers.Authorization = `Bearer ${relaySecret}`;
  }
  return headers;
}
var DISRUPTION_TYPE_MAP = {
  gap_spike: "AIS_DISRUPTION_TYPE_GAP_SPIKE",
  chokepoint_congestion: "AIS_DISRUPTION_TYPE_CHOKEPOINT_CONGESTION"
};
var SEVERITY_MAP = {
  low: "AIS_DISRUPTION_SEVERITY_LOW",
  elevated: "AIS_DISRUPTION_SEVERITY_ELEVATED",
  high: "AIS_DISRUPTION_SEVERITY_HIGH"
};
var SNAPSHOT_CACHE_TTL_MS = 3e5;
var cachedSnapshot;
var cacheTimestamp = 0;
var inFlightRequest = null;
async function fetchVesselSnapshot() {
  const now = Date.now();
  if (cachedSnapshot && now - cacheTimestamp < SNAPSHOT_CACHE_TTL_MS) {
    return cachedSnapshot;
  }
  if (inFlightRequest) {
    return inFlightRequest;
  }
  inFlightRequest = fetchVesselSnapshotFromRelay();
  try {
    const result = await inFlightRequest;
    if (result) {
      cachedSnapshot = result;
      cacheTimestamp = Date.now();
    }
    return result ?? cachedSnapshot;
  } finally {
    inFlightRequest = null;
  }
}
async function fetchVesselSnapshotFromRelay() {
  try {
    const relayBaseUrl = getRelayBaseUrl2();
    if (!relayBaseUrl) return void 0;
    const response = await fetch(
      `${relayBaseUrl}/ais/snapshot?candidates=false`,
      {
        headers: getRelayRequestHeaders(),
        signal: AbortSignal.timeout(1e4)
      }
    );
    if (!response.ok) return void 0;
    const data = await response.json();
    if (!data || !Array.isArray(data.disruptions) || !Array.isArray(data.density)) {
      return void 0;
    }
    const densityZones = data.density.map((z) => ({
      id: String(z.id || ""),
      name: String(z.name || ""),
      location: {
        latitude: Number(z.lat) || 0,
        longitude: Number(z.lon) || 0
      },
      intensity: Number(z.intensity) || 0,
      deltaPct: Number(z.deltaPct) || 0,
      shipsPerDay: Number(z.shipsPerDay) || 0,
      note: String(z.note || "")
    }));
    const disruptions = data.disruptions.map((d) => ({
      id: String(d.id || ""),
      name: String(d.name || ""),
      type: DISRUPTION_TYPE_MAP[d.type] || "AIS_DISRUPTION_TYPE_UNSPECIFIED",
      location: {
        latitude: Number(d.lat) || 0,
        longitude: Number(d.lon) || 0
      },
      severity: SEVERITY_MAP[d.severity] || "AIS_DISRUPTION_SEVERITY_UNSPECIFIED",
      changePct: Number(d.changePct) || 0,
      windowHours: Number(d.windowHours) || 0,
      darkShips: Number(d.darkShips) || 0,
      vesselCount: Number(d.vesselCount) || 0,
      region: String(d.region || ""),
      description: String(d.description || "")
    }));
    return {
      snapshotAt: Date.now(),
      densityZones,
      disruptions
    };
  } catch {
    return void 0;
  }
}
async function getVesselSnapshot(_ctx, _req) {
  try {
    const snapshot = await fetchVesselSnapshot();
    return { snapshot };
  } catch {
    return { snapshot: void 0 };
  }
}

// server/worldmonitor/maritime/v1/list-navigational-warnings.ts
var REDIS_CACHE_KEY12 = "maritime:navwarnings:v1";
var REDIS_CACHE_TTL12 = 3600;
var NGA_WARNINGS_URL = "https://msi.nga.mil/api/publications/broadcast-warn?output=json&status=A";
function parseNgaDate(dateStr) {
  if (!dateStr || typeof dateStr !== "string") return 0;
  const match = dateStr.match(/(\d{2})(\d{4})Z\s+([A-Z]{3})\s+(\d{4})/i);
  if (!match) return Date.parse(dateStr) || 0;
  const months = {
    JAN: 0,
    FEB: 1,
    MAR: 2,
    APR: 3,
    MAY: 4,
    JUN: 5,
    JUL: 6,
    AUG: 7,
    SEP: 8,
    OCT: 9,
    NOV: 10,
    DEC: 11
  };
  const day = parseInt(match[1], 10);
  const hours = parseInt(match[2].slice(0, 2), 10);
  const minutes = parseInt(match[2].slice(2, 4), 10);
  const month = months[match[3].toUpperCase()] ?? 0;
  const year = parseInt(match[4], 10);
  return Date.UTC(year, month, day, hours, minutes);
}
async function fetchNgaWarnings(area) {
  try {
    const response = await fetch(NGA_WARNINGS_URL, {
      headers: { Accept: "application/json", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(15e3)
    });
    if (!response.ok) return [];
    const data = await response.json();
    const rawWarnings = Array.isArray(data) ? data : data?.broadcast_warn ?? [];
    let warnings = rawWarnings.map((w) => ({
      id: `${w.navArea || ""}-${w.msgYear || ""}-${w.msgNumber || ""}`,
      title: `NAVAREA ${w.navArea || ""} ${w.msgNumber || ""}/${w.msgYear || ""}`,
      text: w.text || "",
      area: `${w.navArea || ""}${w.subregion ? " " + w.subregion : ""}`,
      location: void 0,
      issuedAt: parseNgaDate(w.issueDate),
      expiresAt: 0,
      authority: w.authority || ""
    }));
    if (area) {
      const areaLower = area.toLowerCase();
      warnings = warnings.filter(
        (w) => w.area.toLowerCase().includes(areaLower) || w.text.toLowerCase().includes(areaLower)
      );
    }
    return warnings;
  } catch {
    return [];
  }
}
async function listNavigationalWarnings(_ctx, req) {
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY12}:${req.area || "all"}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL12, async () => {
      const warnings = await fetchNgaWarnings(req.area);
      return warnings.length > 0 ? { warnings, pagination: void 0 } : null;
    });
    return result || { warnings: [], pagination: void 0 };
  } catch {
    return { warnings: [], pagination: void 0 };
  }
}

// server/worldmonitor/maritime/v1/handler.ts
var maritimeHandler = {
  getVesselSnapshot,
  listNavigationalWarnings
};

// src/generated/server/worldmonitor/cyber/v1/service_server.ts
var ValidationError11 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createCyberServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/cyber/v1/list-cyber-threats",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            start: Number(params.get("start") ?? "0"),
            end: Number(params.get("end") ?? "0"),
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            type: params.get("type") ?? "CYBER_THREAT_TYPE_UNSPECIFIED",
            source: params.get("source") ?? "CYBER_THREAT_SOURCE_UNSPECIFIED",
            minSeverity: params.get("min_severity") ?? "CRITICALITY_LEVEL_UNSPECIFIED"
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listCyberThreats", body);
            if (bodyViolations) {
              throw new ValidationError11(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listCyberThreats(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError11) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/cyber/v1/_shared.ts
var DEFAULT_LIMIT = 500;
var MAX_LIMIT = 1e3;
var DEFAULT_DAYS = 14;
var MAX_DAYS = 90;
var FEODO_URL = "https://feodotracker.abuse.ch/downloads/ipblocklist.json";
var URLHAUS_RECENT_URL = (limit) => `https://urlhaus-api.abuse.ch/v1/urls/recent/limit/${limit}/`;
var C2INTEL_URL = "https://raw.githubusercontent.com/drb-ra/C2IntelFeeds/master/feeds/IPC2s-30day.csv";
var OTX_INDICATORS_URL = "https://otx.alienvault.com/api/v1/indicators/export?type=IPv4&modified_since=";
var ABUSEIPDB_BLACKLIST_URL = "https://api.abuseipdb.com/api/v2/blacklist";
var UPSTREAM_TIMEOUT_MS = 8e3;
var GEO_MAX_UNRESOLVED = 250;
var GEO_CONCURRENCY = 16;
var GEO_OVERALL_TIMEOUT_MS = 15e3;
var GEO_PER_IP_TIMEOUT_MS = 3e3;
var GEO_CACHE_TTL_MS = 24 * 60 * 60 * 1e3;
function clampInt(value, fallback, min, max) {
  if (!Number.isFinite(value)) return fallback;
  return Math.max(min, Math.min(max, Math.floor(value)));
}
function cleanString(value, maxLen = 120) {
  if (typeof value !== "string") return "";
  return value.trim().replace(/\s+/g, " ").slice(0, maxLen);
}
function toFiniteNumber(value) {
  const parsed = typeof value === "number" ? value : Number.parseFloat(String(value ?? ""));
  return Number.isFinite(parsed) ? parsed : null;
}
function hasValidCoordinates(lat, lon) {
  if (lat === null || lon === null) return false;
  return lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180;
}
function isIPv4(value) {
  if (!/^(\d{1,3}\.){3}\d{1,3}$/.test(value)) return false;
  const octets = value.split(".").map(Number);
  return octets.every((n) => Number.isInteger(n) && n >= 0 && n <= 255);
}
function isIPv6(value) {
  return /^[0-9a-f:]+$/i.test(value) && value.includes(":");
}
function isIpAddress(value) {
  const candidate = cleanString(value, 80).toLowerCase();
  if (!candidate) return false;
  return isIPv4(candidate) || isIPv6(candidate);
}
function normalizeCountry(value) {
  const raw = cleanString(String(value ?? ""), 64);
  if (!raw) return "";
  if (/^[a-z]{2}$/i.test(raw)) return raw.toUpperCase();
  return raw;
}
function toEpochMs(value) {
  if (!value) return 0;
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value.getTime();
  const raw = cleanString(String(value), 80);
  if (!raw) return 0;
  const normalized = raw.replace(" UTC", "Z").replace(" GMT", "Z").replace(" +00:00", "Z").replace(" ", "T");
  const direct = new Date(raw);
  if (!Number.isNaN(direct.getTime())) return direct.getTime();
  const fallback = new Date(normalized);
  if (!Number.isNaN(fallback.getTime())) return fallback.getTime();
  return 0;
}
function normalizeTags(input, maxTags = 8) {
  const tags = Array.isArray(input) ? input : typeof input === "string" ? input.split(/[;,|]/g) : [];
  const normalized = [];
  const seen = /* @__PURE__ */ new Set();
  for (const tag of tags) {
    const clean = cleanString(String(tag ?? ""), 40).toLowerCase();
    if (!clean || seen.has(clean)) continue;
    seen.add(clean);
    normalized.push(clean);
    if (normalized.length >= maxTags) break;
  }
  return normalized;
}
var THREAT_TYPE_MAP = {
  c2_server: "CYBER_THREAT_TYPE_C2_SERVER",
  malware_host: "CYBER_THREAT_TYPE_MALWARE_HOST",
  phishing: "CYBER_THREAT_TYPE_PHISHING",
  malicious_url: "CYBER_THREAT_TYPE_MALICIOUS_URL"
};
var SOURCE_MAP = {
  feodo: "CYBER_THREAT_SOURCE_FEODO",
  urlhaus: "CYBER_THREAT_SOURCE_URLHAUS",
  c2intel: "CYBER_THREAT_SOURCE_C2INTEL",
  otx: "CYBER_THREAT_SOURCE_OTX",
  abuseipdb: "CYBER_THREAT_SOURCE_ABUSEIPDB"
};
var INDICATOR_TYPE_MAP = {
  ip: "CYBER_THREAT_INDICATOR_TYPE_IP",
  domain: "CYBER_THREAT_INDICATOR_TYPE_DOMAIN",
  url: "CYBER_THREAT_INDICATOR_TYPE_URL"
};
var SEVERITY_MAP2 = {
  low: "CRITICALITY_LEVEL_LOW",
  medium: "CRITICALITY_LEVEL_MEDIUM",
  high: "CRITICALITY_LEVEL_HIGH",
  critical: "CRITICALITY_LEVEL_CRITICAL"
};
var SEVERITY_RANK = {
  CRITICALITY_LEVEL_CRITICAL: 4,
  CRITICALITY_LEVEL_HIGH: 3,
  CRITICALITY_LEVEL_MEDIUM: 2,
  CRITICALITY_LEVEL_LOW: 1,
  CRITICALITY_LEVEL_UNSPECIFIED: 0
};
var COUNTRY_CENTROIDS2 = {
  US: [39.8, -98.6],
  CA: [56.1, -106.3],
  MX: [23.6, -102.6],
  BR: [-14.2, -51.9],
  AR: [-38.4, -63.6],
  GB: [55.4, -3.4],
  DE: [51.2, 10.5],
  FR: [46.2, 2.2],
  IT: [41.9, 12.6],
  ES: [40.5, -3.7],
  NL: [52.1, 5.3],
  BE: [50.5, 4.5],
  SE: [60.1, 18.6],
  NO: [60.5, 8.5],
  FI: [61.9, 25.7],
  DK: [56.3, 9.5],
  PL: [51.9, 19.1],
  CZ: [49.8, 15.5],
  AT: [47.5, 14.6],
  CH: [46.8, 8.2],
  PT: [39.4, -8.2],
  IE: [53.1, -8.2],
  RO: [45.9, 25],
  HU: [47.2, 19.5],
  BG: [42.7, 25.5],
  HR: [45.1, 15.2],
  SK: [48.7, 19.7],
  UA: [48.4, 31.2],
  RU: [61.5, 105.3],
  BY: [53.7, 28],
  TR: [39, 35.2],
  GR: [39.1, 21.8],
  RS: [44, 21],
  CN: [35.9, 104.2],
  JP: [36.2, 138.3],
  KR: [35.9, 127.8],
  IN: [20.6, 79],
  PK: [30.4, 69.3],
  BD: [23.7, 90.4],
  ID: [-0.8, 113.9],
  TH: [15.9, 101],
  VN: [14.1, 108.3],
  PH: [12.9, 121.8],
  MY: [4.2, 101.9],
  SG: [1.4, 103.8],
  TW: [23.7, 121],
  HK: [22.4, 114.1],
  AU: [-25.3, 133.8],
  NZ: [-40.9, 174.9],
  ZA: [-30.6, 22.9],
  NG: [9.1, 8.7],
  EG: [26.8, 30.8],
  KE: [-0.02, 37.9],
  ET: [9.1, 40.5],
  MA: [31.8, -7.1],
  DZ: [28, 1.7],
  TN: [33.9, 9.5],
  GH: [7.9, -1],
  SA: [23.9, 45.1],
  AE: [23.4, 53.8],
  IL: [31, 34.9],
  IR: [32.4, 53.7],
  IQ: [33.2, 43.7],
  KW: [29.3, 47.5],
  QA: [25.4, 51.2],
  BH: [26, 50.6],
  JO: [30.6, 36.2],
  LB: [33.9, 35.9],
  CL: [-35.7, -71.5],
  CO: [4.6, -74.3],
  PE: [-9.2, -75],
  VE: [6.4, -66.6],
  KZ: [48, 68],
  UZ: [41.4, 64.6],
  GE: [42.3, 43.4],
  AZ: [40.1, 47.6],
  AM: [40.1, 45],
  LT: [55.2, 23.9],
  LV: [56.9, 24.1],
  EE: [58.6, 25],
  HN: [15.2, -86.2],
  GT: [15.8, -90.2],
  PA: [8.5, -80.8],
  CR: [9.7, -84],
  SN: [14.5, -14.5],
  CM: [7.4, 12.4],
  CI: [7.5, -5.5],
  TZ: [-6.4, 34.9],
  UG: [1.4, 32.3]
};
function djb2(seed) {
  let h = 5381;
  for (let i = 0; i < seed.length; i++) h = (h << 5) + h + seed.charCodeAt(i) & 4294967295;
  return h;
}
function getCountryCentroid(countryCode, seed) {
  if (!countryCode) return null;
  const coords = COUNTRY_CENTROIDS2[countryCode.toUpperCase()];
  if (!coords) return null;
  const key = seed || countryCode;
  const latOffset = ((djb2(key) & 65535) / 65535 - 0.5) * 2;
  const lonOffset = ((djb2(key + ":lon") & 65535) / 65535 - 0.5) * 2;
  return { lat: coords[0] + latOffset, lon: coords[1] + lonOffset };
}
function sanitizeRawThreat(threat) {
  const indicator = cleanString(threat.indicator, 255);
  if (!indicator) return null;
  const indicatorType = threat.indicatorType || "ip";
  if (indicatorType === "ip" && !isIpAddress(indicator)) return null;
  return {
    id: cleanString(threat.id, 255) || `${threat.source || "feodo"}:${indicatorType}:${indicator}`,
    type: threat.type || "malicious_url",
    source: threat.source || "feodo",
    indicator,
    indicatorType,
    lat: threat.lat ?? null,
    lon: threat.lon ?? null,
    country: threat.country || "",
    severity: threat.severity || "medium",
    malwareFamily: cleanString(threat.malwareFamily, 80),
    tags: threat.tags || [],
    firstSeen: threat.firstSeen || 0,
    lastSeen: threat.lastSeen || 0
  };
}
var GEO_CACHE_MAX_SIZE = 2048;
var geoCache = /* @__PURE__ */ new Map();
function getGeoCached(ip) {
  const entry = geoCache.get(ip);
  if (!entry) return null;
  if (Date.now() - entry.ts > GEO_CACHE_TTL_MS) {
    geoCache.delete(ip);
    return null;
  }
  return entry;
}
function setGeoCached(ip, geo) {
  if (geoCache.size >= GEO_CACHE_MAX_SIZE) {
    const keysToDelete = Array.from(geoCache.keys()).slice(0, Math.floor(GEO_CACHE_MAX_SIZE / 4));
    for (const key of keysToDelete) geoCache.delete(key);
  }
  geoCache.set(ip, { ...geo, ts: Date.now() });
}
async function fetchGeoIp(ip, signal) {
  try {
    const resp = await fetch(`https://ipinfo.io/${encodeURIComponent(ip)}/json`, {
      headers: { "User-Agent": CHROME_UA },
      signal: signal || AbortSignal.timeout(GEO_PER_IP_TIMEOUT_MS)
    });
    if (resp.ok) {
      const data = await resp.json();
      const parts = (data.loc || "").split(",");
      const lat = toFiniteNumber(parts[0]);
      const lon = toFiniteNumber(parts[1]);
      if (hasValidCoordinates(lat, lon)) {
        return { lat, lon, country: normalizeCountry(data.country) };
      }
    }
  } catch {
  }
  if (signal?.aborted) return null;
  try {
    const resp = await fetch(`https://freeipapi.com/api/json/${encodeURIComponent(ip)}`, {
      headers: { "User-Agent": CHROME_UA },
      signal: signal || AbortSignal.timeout(GEO_PER_IP_TIMEOUT_MS)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    const lat = toFiniteNumber(data.latitude);
    const lon = toFiniteNumber(data.longitude);
    if (!hasValidCoordinates(lat, lon)) return null;
    return { lat, lon, country: normalizeCountry(data.countryCode || data.countryName) };
  } catch {
    return null;
  }
}
async function geolocateIp(ip, signal) {
  const cached = getGeoCached(ip);
  if (cached) return cached;
  const geo = await fetchGeoIp(ip, signal);
  if (geo) setGeoCached(ip, geo);
  return geo;
}
async function hydrateThreatCoordinates(threats) {
  const unresolvedIps = [];
  const seenIps = /* @__PURE__ */ new Set();
  for (const threat of threats) {
    if (hasValidCoordinates(threat.lat, threat.lon)) continue;
    if (threat.indicatorType !== "ip") continue;
    const ip = cleanString(threat.indicator, 80).toLowerCase();
    if (!isIpAddress(ip) || seenIps.has(ip)) continue;
    seenIps.add(ip);
    unresolvedIps.push(ip);
  }
  const capped = unresolvedIps.slice(0, GEO_MAX_UNRESOLVED);
  const resolvedByIp = /* @__PURE__ */ new Map();
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), GEO_OVERALL_TIMEOUT_MS);
  const queue = [...capped];
  const workerCount = Math.min(GEO_CONCURRENCY, queue.length);
  const workers = Array.from({ length: workerCount }, async () => {
    while (queue.length > 0 && !controller.signal.aborted) {
      const ip = queue.shift();
      if (!ip) continue;
      const geo = await geolocateIp(ip, controller.signal);
      if (geo) resolvedByIp.set(ip, geo);
    }
  });
  try {
    await Promise.all(workers);
  } catch {
  }
  clearTimeout(timeoutId);
  return threats.map((threat) => {
    if (hasValidCoordinates(threat.lat, threat.lon)) return threat;
    if (threat.indicatorType !== "ip") return threat;
    const lookup = resolvedByIp.get(cleanString(threat.indicator, 80).toLowerCase());
    if (lookup) {
      return { ...threat, lat: lookup.lat, lon: lookup.lon, country: threat.country || lookup.country };
    }
    const centroid = getCountryCentroid(threat.country, threat.indicator);
    if (centroid) {
      return { ...threat, lat: centroid.lat, lon: centroid.lon };
    }
    return threat;
  });
}
function inferFeodoSeverity(record, malwareFamily) {
  if (/emotet|qakbot|trickbot|dridex|ransom/i.test(malwareFamily)) return "critical";
  const status = cleanString(record?.status || record?.c2_status || "", 30).toLowerCase();
  if (status === "online") return "high";
  return "medium";
}
function parseFeodoRecord(record, cutoffMs) {
  const ip = cleanString(
    record?.ip_address || record?.dst_ip || record?.ip || record?.ioc || record?.host,
    80
  ).toLowerCase();
  if (!isIpAddress(ip)) return null;
  const statusRaw = cleanString(record?.status || record?.c2_status || "", 30).toLowerCase();
  if (statusRaw && statusRaw !== "online" && statusRaw !== "offline") return null;
  const firstSeen = toEpochMs(record?.first_seen || record?.first_seen_utc || record?.dateadded);
  const lastSeen = toEpochMs(record?.last_online || record?.last_seen || record?.last_seen_utc || record?.first_seen || record?.first_seen_utc);
  const activityMs = lastSeen || firstSeen;
  if (activityMs && activityMs < cutoffMs) return null;
  const malwareFamily = cleanString(record?.malware || record?.malware_family || record?.family, 80);
  const tags = normalizeTags(record?.tags);
  return sanitizeRawThreat({
    id: `feodo:${ip}`,
    type: "c2_server",
    source: "feodo",
    indicator: ip,
    indicatorType: "ip",
    lat: toFiniteNumber(record?.latitude ?? record?.lat),
    lon: toFiniteNumber(record?.longitude ?? record?.lon),
    country: normalizeCountry(record?.country || record?.country_code),
    severity: statusRaw === "online" ? inferFeodoSeverity(record, malwareFamily) : "medium",
    malwareFamily,
    tags: normalizeTags(["botnet", "c2", ...tags]),
    firstSeen,
    lastSeen
  });
}
async function fetchFeodoSource(limit, cutoffMs) {
  try {
    const response = await fetch(FEODO_URL, {
      headers: { Accept: "application/json", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS)
    });
    if (!response.ok) return { ok: false, threats: [] };
    const payload = await response.json();
    const records = Array.isArray(payload) ? payload : Array.isArray(payload?.data) ? payload.data : [];
    const parsed = records.map((r) => parseFeodoRecord(r, cutoffMs)).filter((t) => t !== null).sort((a, b) => (b.lastSeen || b.firstSeen) - (a.lastSeen || a.firstSeen)).slice(0, limit);
    return { ok: true, threats: parsed };
  } catch {
    return { ok: false, threats: [] };
  }
}
function inferUrlhausType(record, tags) {
  const threat = cleanString(record?.threat || record?.threat_type || "", 40).toLowerCase();
  const allTags = tags.join(" ");
  if (threat.includes("phish") || allTags.includes("phish")) return "phishing";
  if (threat.includes("malware") || threat.includes("payload") || allTags.includes("malware")) return "malware_host";
  return "malicious_url";
}
function inferUrlhausSeverity(type, tags) {
  if (type === "phishing") return "medium";
  if (tags.includes("ransomware") || tags.includes("botnet")) return "critical";
  if (type === "malware_host") return "high";
  return "medium";
}
function parseUrlhausRecord(record, cutoffMs) {
  const rawUrl = cleanString(record?.url || record?.ioc || "", 1024);
  const statusRaw = cleanString(record?.url_status || record?.status || "", 30).toLowerCase();
  if (statusRaw && statusRaw !== "online") return null;
  const tags = normalizeTags(record?.tags);
  let hostname = "";
  if (rawUrl) {
    try {
      hostname = cleanString(new URL(rawUrl).hostname, 255).toLowerCase();
    } catch {
    }
  }
  const recordIp = cleanString(record?.host || record?.ip_address || record?.ip, 80).toLowerCase();
  const ipCandidate = isIpAddress(recordIp) ? recordIp : isIpAddress(hostname) ? hostname : "";
  const indicatorType = ipCandidate ? "ip" : hostname ? "domain" : "url";
  const indicator = ipCandidate || hostname || rawUrl;
  if (!indicator) return null;
  const firstSeen = toEpochMs(record?.dateadded || record?.firstseen || record?.first_seen);
  const lastSeen = toEpochMs(record?.last_online || record?.last_seen || record?.dateadded);
  const activityMs = lastSeen || firstSeen;
  if (activityMs && activityMs < cutoffMs) return null;
  const type = inferUrlhausType(record, tags);
  return sanitizeRawThreat({
    id: `urlhaus:${indicatorType}:${indicator}`,
    type,
    source: "urlhaus",
    indicator,
    indicatorType,
    lat: toFiniteNumber(record?.latitude ?? record?.lat),
    lon: toFiniteNumber(record?.longitude ?? record?.lon),
    country: normalizeCountry(record?.country || record?.country_code),
    severity: inferUrlhausSeverity(type, tags),
    malwareFamily: cleanString(record?.threat, 80),
    tags,
    firstSeen,
    lastSeen
  });
}
async function fetchUrlhausSource(limit, cutoffMs) {
  const authKey = cleanString(process.env.URLHAUS_AUTH_KEY || "", 200);
  if (!authKey) return { ok: false, threats: [] };
  try {
    const response = await fetch(URLHAUS_RECENT_URL(limit), {
      method: "GET",
      headers: { Accept: "application/json", "Auth-Key": authKey, "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS)
    });
    if (!response.ok) return { ok: false, threats: [] };
    const payload = await response.json();
    const rows = Array.isArray(payload?.urls) ? payload.urls : Array.isArray(payload?.data) ? payload.data : [];
    const parsed = rows.map((r) => parseUrlhausRecord(r, cutoffMs)).filter((t) => t !== null).sort((a, b) => (b.lastSeen || b.firstSeen) - (a.lastSeen || a.firstSeen)).slice(0, limit);
    return { ok: true, threats: parsed };
  } catch {
    return { ok: false, threats: [] };
  }
}
function parseC2IntelCsvLine(line) {
  if (!line || line.startsWith("#")) return null;
  const commaIdx = line.indexOf(",");
  if (commaIdx < 0) return null;
  const ip = cleanString(line.slice(0, commaIdx), 80).toLowerCase();
  if (!isIpAddress(ip)) return null;
  const description = cleanString(line.slice(commaIdx + 1), 200);
  const malwareFamily = description.replace(/^Possible\s+/i, "").replace(/\s+C2\s+IP$/i, "").trim() || "Unknown";
  const tags = ["c2"];
  const descLower = description.toLowerCase();
  if (descLower.includes("cobaltstrike") || descLower.includes("cobalt strike")) tags.push("cobaltstrike");
  if (descLower.includes("metasploit")) tags.push("metasploit");
  if (descLower.includes("sliver")) tags.push("sliver");
  if (descLower.includes("brute ratel") || descLower.includes("bruteratel")) tags.push("bruteratel");
  const severity = /cobaltstrike|cobalt.strike|brute.?ratel/i.test(description) ? "high" : "medium";
  return sanitizeRawThreat({
    id: `c2intel:${ip}`,
    type: "c2_server",
    source: "c2intel",
    indicator: ip,
    indicatorType: "ip",
    lat: null,
    lon: null,
    country: "",
    severity,
    malwareFamily,
    tags: normalizeTags(tags),
    firstSeen: 0,
    lastSeen: 0
  });
}
async function fetchC2IntelSource(limit) {
  try {
    const response = await fetch(C2INTEL_URL, {
      headers: { Accept: "text/plain", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS)
    });
    if (!response.ok) return { ok: false, threats: [] };
    const text = await response.text();
    const parsed = text.split("\n").map((line) => parseC2IntelCsvLine(line)).filter((t) => t !== null).slice(0, limit);
    return { ok: true, threats: parsed };
  } catch {
    return { ok: false, threats: [] };
  }
}
async function fetchOtxSource(limit, days) {
  const apiKey = cleanString(process.env.OTX_API_KEY || "", 200);
  if (!apiKey) return { ok: false, threats: [] };
  try {
    const since = new Date(Date.now() - days * 24 * 60 * 60 * 1e3).toISOString().slice(0, 10);
    const response = await fetch(
      `${OTX_INDICATORS_URL}${encodeURIComponent(since)}`,
      {
        headers: { Accept: "application/json", "X-OTX-API-KEY": apiKey, "User-Agent": CHROME_UA },
        signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS)
      }
    );
    if (!response.ok) return { ok: false, threats: [] };
    const payload = await response.json();
    const results = Array.isArray(payload?.results) ? payload.results : Array.isArray(payload) ? payload : [];
    const parsed = [];
    for (const record of results) {
      const ip = cleanString(record?.indicator || record?.ip || "", 80).toLowerCase();
      if (!isIpAddress(ip)) continue;
      const title = cleanString(record?.title || record?.description || "", 200);
      const tags = normalizeTags(record?.tags || []);
      const severity = tags.some((t) => /ransomware|apt|c2|botnet/.test(t)) ? "high" : "medium";
      const threat = sanitizeRawThreat({
        id: `otx:${ip}`,
        type: tags.some((t) => /c2|botnet/.test(t)) ? "c2_server" : "malware_host",
        source: "otx",
        indicator: ip,
        indicatorType: "ip",
        lat: null,
        lon: null,
        country: "",
        severity,
        malwareFamily: title,
        tags,
        firstSeen: toEpochMs(record?.created),
        lastSeen: toEpochMs(record?.modified || record?.created)
      });
      if (threat) parsed.push(threat);
      if (parsed.length >= limit) break;
    }
    return { ok: true, threats: parsed };
  } catch {
    return { ok: false, threats: [] };
  }
}
async function fetchAbuseIpDbSource(limit) {
  const apiKey = cleanString(process.env.ABUSEIPDB_API_KEY || "", 200);
  if (!apiKey) return { ok: false, threats: [] };
  try {
    const url = `${ABUSEIPDB_BLACKLIST_URL}?confidenceMinimum=90&limit=${Math.min(limit, 500)}`;
    const response = await fetch(url, {
      headers: { Accept: "application/json", Key: apiKey, "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS)
    });
    if (!response.ok) return { ok: false, threats: [] };
    const payload = await response.json();
    const records = Array.isArray(payload?.data) ? payload.data : [];
    const parsed = [];
    for (const record of records) {
      const ip = cleanString(record?.ipAddress || record?.ip || "", 80).toLowerCase();
      if (!isIpAddress(ip)) continue;
      const score = toFiniteNumber(record?.abuseConfidenceScore) ?? 0;
      const severity = score >= 95 ? "critical" : score >= 80 ? "high" : "medium";
      const threat = sanitizeRawThreat({
        id: `abuseipdb:${ip}`,
        type: "malware_host",
        source: "abuseipdb",
        indicator: ip,
        indicatorType: "ip",
        lat: toFiniteNumber(record?.latitude ?? record?.lat),
        lon: toFiniteNumber(record?.longitude ?? record?.lon),
        country: normalizeCountry(record?.countryCode || record?.country),
        severity,
        malwareFamily: "",
        tags: normalizeTags([`score:${score}`]),
        firstSeen: 0,
        lastSeen: toEpochMs(record?.lastReportedAt)
      });
      if (threat) parsed.push(threat);
      if (parsed.length >= limit) break;
    }
    return { ok: true, threats: parsed };
  } catch {
    return { ok: false, threats: [] };
  }
}
function dedupeThreats(threats) {
  const deduped = /* @__PURE__ */ new Map();
  for (const threat of threats) {
    const key = `${threat.source}:${threat.indicatorType}:${threat.indicator}`;
    const existing = deduped.get(key);
    if (!existing) {
      deduped.set(key, threat);
      continue;
    }
    const existingSeen = existing.lastSeen || existing.firstSeen;
    const candidateSeen = threat.lastSeen || threat.firstSeen;
    if (candidateSeen >= existingSeen) {
      deduped.set(key, {
        ...existing,
        ...threat,
        tags: normalizeTags([...existing.tags, ...threat.tags])
      });
    }
  }
  return Array.from(deduped.values());
}
function toProtoCyberThreat(raw) {
  return {
    id: raw.id,
    type: THREAT_TYPE_MAP[raw.type] || "CYBER_THREAT_TYPE_UNSPECIFIED",
    source: SOURCE_MAP[raw.source] || "CYBER_THREAT_SOURCE_UNSPECIFIED",
    indicator: raw.indicator,
    indicatorType: INDICATOR_TYPE_MAP[raw.indicatorType] || "CYBER_THREAT_INDICATOR_TYPE_UNSPECIFIED",
    location: hasValidCoordinates(raw.lat, raw.lon) ? { latitude: raw.lat, longitude: raw.lon } : void 0,
    country: raw.country,
    severity: SEVERITY_MAP2[raw.severity] || "CRITICALITY_LEVEL_UNSPECIFIED",
    malwareFamily: raw.malwareFamily,
    tags: raw.tags,
    firstSeenAt: raw.firstSeen,
    lastSeenAt: raw.lastSeen
  };
}

// server/worldmonitor/cyber/v1/list-cyber-threats.ts
var REDIS_CACHE_KEY13 = "cyber:threats:v1";
var REDIS_CACHE_TTL13 = 900;
async function listCyberThreats(_ctx, req) {
  try {
    const now = Date.now();
    const pageSize = clampInt(req.pageSize, DEFAULT_LIMIT, 1, MAX_LIMIT);
    const cacheKey2 = `${REDIS_CACHE_KEY13}:${pageSize}:${req.start || 0}:${req.type || ""}:${req.source || ""}:${req.minSeverity || ""}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL13, async () => {
      let days = DEFAULT_DAYS;
      if (req.start) {
        days = clampInt(
          Math.ceil((now - req.start) / (24 * 60 * 60 * 1e3)),
          DEFAULT_DAYS,
          1,
          MAX_DAYS
        );
      }
      const cutoffMs = now - days * 24 * 60 * 60 * 1e3;
      const [feodo, urlhaus, c2intel, otx, abuseipdb] = await Promise.all([
        fetchFeodoSource(pageSize, cutoffMs),
        fetchUrlhausSource(pageSize, cutoffMs),
        fetchC2IntelSource(pageSize),
        fetchOtxSource(pageSize, days),
        fetchAbuseIpDbSource(pageSize)
      ]);
      const anySucceeded = feodo.ok || urlhaus.ok || c2intel.ok || otx.ok || abuseipdb.ok;
      if (!anySucceeded) return null;
      const combined = dedupeThreats([
        ...feodo.threats,
        ...urlhaus.threats,
        ...c2intel.threats,
        ...otx.threats,
        ...abuseipdb.threats
      ]);
      const hydrated = await hydrateThreatCoordinates(combined);
      let results = hydrated.filter((t) => t.lat !== null && t.lon !== null && t.lat >= -90 && t.lat <= 90 && t.lon >= -180 && t.lon <= 180);
      if (req.type && req.type !== "CYBER_THREAT_TYPE_UNSPECIFIED") {
        const filterType = req.type;
        results = results.filter((t) => THREAT_TYPE_MAP[t.type] === filterType);
      }
      if (req.source && req.source !== "CYBER_THREAT_SOURCE_UNSPECIFIED") {
        const filterSource = req.source;
        results = results.filter((t) => SOURCE_MAP[t.source] === filterSource);
      }
      if (req.minSeverity && req.minSeverity !== "CRITICALITY_LEVEL_UNSPECIFIED") {
        const minRank = SEVERITY_RANK[req.minSeverity] || 0;
        results = results.filter((t) => (SEVERITY_RANK[SEVERITY_MAP2[t.severity] || ""] || 0) >= minRank);
      }
      results = results.sort((a, b) => {
        const bySeverity = (SEVERITY_RANK[SEVERITY_MAP2[b.severity] || ""] || 0) - (SEVERITY_RANK[SEVERITY_MAP2[a.severity] || ""] || 0);
        if (bySeverity !== 0) return bySeverity;
        return (b.lastSeen || b.firstSeen) - (a.lastSeen || a.firstSeen);
      }).slice(0, pageSize);
      const threats = results.map(toProtoCyberThreat);
      return threats.length > 0 ? { threats, pagination: void 0 } : null;
    });
    return result || { threats: [], pagination: void 0 };
  } catch {
    return { threats: [], pagination: void 0 };
  }
}

// server/worldmonitor/cyber/v1/handler.ts
var cyberHandler = {
  listCyberThreats
};

// src/generated/server/worldmonitor/economic/v1/service_server.ts
var ValidationError12 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createEconomicServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/economic/v1/get-fred-series",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            seriesId: params.get("series_id") ?? "",
            limit: Number(params.get("limit") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getFredSeries", body);
            if (bodyViolations) {
              throw new ValidationError12(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getFredSeries(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError12) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/economic/v1/list-world-bank-indicators",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            indicatorCode: params.get("indicator_code") ?? "",
            countryCode: params.get("country_code") ?? "",
            year: Number(params.get("year") ?? "0"),
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listWorldBankIndicators", body);
            if (bodyViolations) {
              throw new ValidationError12(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listWorldBankIndicators(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError12) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/economic/v1/get-energy-prices",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            commodities: params.getAll("commodities")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getEnergyPrices", body);
            if (bodyViolations) {
              throw new ValidationError12(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getEnergyPrices(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError12) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/economic/v1/get-macro-signals",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getMacroSignals(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError12) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/economic/v1/get-energy-capacity",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            energySources: params.getAll("energy_sources"),
            years: Number(params.get("years") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getEnergyCapacity", body);
            if (bodyViolations) {
              throw new ValidationError12(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getEnergyCapacity(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError12) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/economic/v1/get-bis-policy-rates",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getBisPolicyRates(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError12) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/economic/v1/get-bis-exchange-rates",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getBisExchangeRates(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError12) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/economic/v1/get-bis-credit",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getBisCredit(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError12) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/economic/v1/get-fred-series.ts
var FRED_API_BASE = "https://api.stlouisfed.org/fred";
var REDIS_CACHE_KEY14 = "economic:fred:v1";
var REDIS_CACHE_TTL14 = 3600;
async function fetchFredSeries(req) {
  try {
    const apiKey = process.env.FRED_API_KEY;
    if (!apiKey) return void 0;
    const limit = req.limit > 0 ? Math.min(req.limit, 1e3) : 120;
    const obsParams = new URLSearchParams({
      series_id: req.seriesId,
      api_key: apiKey,
      file_type: "json",
      sort_order: "desc",
      limit: String(limit)
    });
    const metaParams = new URLSearchParams({
      series_id: req.seriesId,
      api_key: apiKey,
      file_type: "json"
    });
    const [obsResponse, metaResponse] = await Promise.all([
      fetch(`${FRED_API_BASE}/series/observations?${obsParams}`, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(1e4)
      }),
      fetch(`${FRED_API_BASE}/series?${metaParams}`, {
        headers: { Accept: "application/json" },
        signal: AbortSignal.timeout(1e4)
      })
    ]);
    if (!obsResponse.ok) return void 0;
    const obsData = await obsResponse.json();
    const observations = (obsData.observations || []).map((obs) => {
      const value = parseFloat(obs.value);
      if (isNaN(value) || obs.value === ".") return null;
      return { date: obs.date, value };
    }).filter((o) => o !== null).reverse();
    let title = req.seriesId;
    let units = "";
    let frequency = "";
    if (metaResponse.ok) {
      const metaData = await metaResponse.json();
      const meta = metaData.seriess?.[0];
      if (meta) {
        title = meta.title || req.seriesId;
        units = meta.units || "";
        frequency = meta.frequency || "";
      }
    }
    return {
      seriesId: req.seriesId,
      title,
      units,
      frequency,
      observations
    };
  } catch {
    return void 0;
  }
}
async function getFredSeries(_ctx, req) {
  if (!req.seriesId) return { series: void 0 };
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY14}:${req.seriesId}:${req.limit || 0}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL14, async () => {
      const series = await fetchFredSeries(req);
      return series ? { series } : null;
    });
    return result || { series: void 0 };
  } catch {
    return { series: void 0 };
  }
}

// server/worldmonitor/economic/v1/list-world-bank-indicators.ts
var REDIS_CACHE_KEY15 = "economic:worldbank:v1";
var REDIS_CACHE_TTL15 = 86400;
var TECH_COUNTRIES = [
  "USA",
  "CHN",
  "JPN",
  "DEU",
  "KOR",
  "GBR",
  "IND",
  "ISR",
  "SGP",
  "TWN",
  "FRA",
  "CAN",
  "SWE",
  "NLD",
  "CHE",
  "FIN",
  "IRL",
  "AUS",
  "BRA",
  "IDN",
  "ARE",
  "SAU",
  "QAT",
  "BHR",
  "EGY",
  "TUR",
  "MYS",
  "THA",
  "VNM",
  "PHL",
  "ESP",
  "ITA",
  "POL",
  "CZE",
  "DNK",
  "NOR",
  "AUT",
  "BEL",
  "PRT",
  "EST",
  "MEX",
  "ARG",
  "CHL",
  "COL",
  "ZAF",
  "NGA",
  "KEN"
];
async function fetchWorldBankIndicators(req) {
  try {
    const indicator = req.indicatorCode;
    if (!indicator) return [];
    const countryList = req.countryCode || TECH_COUNTRIES.join(";");
    const currentYear = (/* @__PURE__ */ new Date()).getFullYear();
    const years = req.year > 0 ? req.year : 5;
    const startYear = currentYear - years;
    const wbUrl = `https://api.worldbank.org/v2/country/${countryList}/indicator/${indicator}?format=json&date=${startYear}:${currentYear}&per_page=1000`;
    const response = await fetch(wbUrl, {
      headers: {
        Accept: "application/json",
        "User-Agent": CHROME_UA
      },
      signal: AbortSignal.timeout(15e3)
    });
    if (!response.ok) return [];
    const data = await response.json();
    if (!data || !Array.isArray(data) || data.length < 2 || !data[1]) return [];
    const records = data[1];
    const indicatorName = records[0]?.indicator?.value || indicator;
    return records.filter((r) => r.countryiso3code && r.value !== null).map((r) => ({
      countryCode: r.countryiso3code || r.country?.id || "",
      countryName: r.country?.value || "",
      indicatorCode: indicator,
      indicatorName,
      year: parseInt(r.date, 10) || 0,
      value: r.value
    }));
  } catch {
    return [];
  }
}
async function listWorldBankIndicators(_ctx, req) {
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY15}:${req.indicatorCode}:${req.countryCode || "all"}:${req.year || 0}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL15, async () => {
      const data = await fetchWorldBankIndicators(req);
      return data.length > 0 ? { data, pagination: void 0 } : null;
    });
    return result || { data: [], pagination: void 0 };
  } catch {
    return { data: [], pagination: void 0 };
  }
}

// server/worldmonitor/economic/v1/get-energy-prices.ts
var REDIS_CACHE_KEY16 = "economic:energy:v1";
var REDIS_CACHE_TTL16 = 3600;
var EIA_SERIES = [
  {
    commodity: "wti",
    name: "WTI Crude Oil",
    unit: "$/barrel",
    apiPath: "/v2/petroleum/pri/spt/data/",
    seriesFacet: "RWTC"
  },
  {
    commodity: "brent",
    name: "Brent Crude Oil",
    unit: "$/barrel",
    apiPath: "/v2/petroleum/pri/spt/data/",
    seriesFacet: "RBRTE"
  }
];
async function fetchEiaSeries(config2, apiKey) {
  try {
    const params = new URLSearchParams({
      api_key: apiKey,
      "data[]": "value",
      frequency: "weekly",
      "facets[series][]": config2.seriesFacet,
      "sort[0][column]": "period",
      "sort[0][direction]": "desc",
      length: "2"
    });
    const response = await fetch(`https://api.eia.gov${config2.apiPath}?${params}`, {
      headers: { Accept: "application/json", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(1e4)
    });
    if (!response.ok) return null;
    const data = await response.json();
    const rows = data.response?.data;
    if (!rows || rows.length === 0) return null;
    const current = rows[0];
    const previous = rows[1];
    const price = current.value ?? 0;
    const prevPrice = previous?.value ?? price;
    const change = prevPrice !== 0 ? (price - prevPrice) / prevPrice * 100 : 0;
    const priceAt = current.period ? new Date(current.period).getTime() : Date.now();
    return {
      commodity: config2.commodity,
      name: config2.name,
      price,
      unit: config2.unit,
      change: Math.round(change * 10) / 10,
      priceAt: Number.isFinite(priceAt) ? priceAt : Date.now()
    };
  } catch {
    return null;
  }
}
async function fetchEnergyPrices(commodities) {
  const apiKey = process.env.EIA_API_KEY;
  if (!apiKey) return [];
  const series = commodities.length > 0 ? EIA_SERIES.filter((s) => commodities.includes(s.commodity)) : EIA_SERIES;
  const results = await Promise.all(series.map((s) => fetchEiaSeries(s, apiKey)));
  return results.filter((p) => p !== null);
}
async function getEnergyPrices(_ctx, req) {
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY16}:${[...req.commodities].sort().join(",") || "all"}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL16, async () => {
      const prices = await fetchEnergyPrices(req.commodities);
      return prices.length > 0 ? { prices } : null;
    });
    return result || { prices: [] };
  } catch {
    return { prices: [] };
  }
}

// server/worldmonitor/economic/v1/_shared.ts
async function fetchJSON(url, timeout = 8e3) {
  if (url.includes("yahoo.com")) await yahooGate();
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, { headers: { "User-Agent": CHROME_UA }, signal: controller.signal });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    return await res.json();
  } finally {
    clearTimeout(id);
  }
}
function rateOfChange(prices, days) {
  if (!prices || prices.length < days + 1) return null;
  const recent = prices[prices.length - 1];
  const past = prices[prices.length - 1 - days];
  if (!past || past === 0) return null;
  return (recent - past) / past * 100;
}
function smaCalc(prices, period) {
  if (!prices || prices.length < period) return null;
  const slice = prices.slice(-period);
  return slice.reduce((a, b) => a + b, 0) / period;
}
function extractClosePrices(chart) {
  try {
    const result = chart?.chart?.result?.[0];
    return result?.indicators?.quote?.[0]?.close?.filter((p) => p != null) || [];
  } catch {
    return [];
  }
}
function extractAlignedPriceVolume(chart) {
  try {
    const result = chart?.chart?.result?.[0];
    const closes = result?.indicators?.quote?.[0]?.close || [];
    const volumes = result?.indicators?.quote?.[0]?.volume || [];
    const pairs = [];
    for (let i = 0; i < closes.length; i++) {
      if (closes[i] != null && volumes[i] != null) {
        pairs.push({ price: closes[i], volume: volumes[i] });
      }
    }
    return pairs;
  } catch {
    return [];
  }
}

// server/worldmonitor/economic/v1/get-macro-signals.ts
var REDIS_CACHE_KEY17 = "economic:macro-signals:v1";
var REDIS_CACHE_TTL17 = 900;
var MACRO_CACHE_TTL = 900;
var macroSignalsCached = null;
var macroSignalsCacheTimestamp = 0;
function buildFallbackResult() {
  return {
    timestamp: (/* @__PURE__ */ new Date()).toISOString(),
    verdict: "UNKNOWN",
    bullishCount: 0,
    totalCount: 0,
    signals: {
      liquidity: { status: "UNKNOWN", sparkline: [] },
      flowStructure: { status: "UNKNOWN" },
      macroRegime: { status: "UNKNOWN" },
      technicalTrend: { status: "UNKNOWN", sparkline: [] },
      hashRate: { status: "UNKNOWN" },
      miningCost: { status: "UNKNOWN" },
      fearGreed: { status: "UNKNOWN", history: [] }
    },
    meta: { qqqSparkline: [] },
    unavailable: true
  };
}
async function computeMacroSignals() {
  const yahooBase = "https://query1.finance.yahoo.com/v8/finance/chart";
  const jpyChart = await fetchJSON(`${yahooBase}/JPY=X?range=1y&interval=1d`).catch(() => null);
  const btcChart = await fetchJSON(`${yahooBase}/BTC-USD?range=1y&interval=1d`).catch(() => null);
  const qqqChart = await fetchJSON(`${yahooBase}/QQQ?range=1y&interval=1d`).catch(() => null);
  const xlpChart = await fetchJSON(`${yahooBase}/XLP?range=1y&interval=1d`).catch(() => null);
  const [fearGreed, mempoolHash] = await Promise.allSettled([
    fetchJSON("https://api.alternative.me/fng/?limit=30&format=json"),
    fetchJSON("https://mempool.space/api/v1/mining/hashrate/1m")
  ]);
  const jpyPrices = jpyChart ? extractClosePrices(jpyChart) : [];
  const btcPrices = btcChart ? extractClosePrices(btcChart) : [];
  const btcAligned = btcChart ? extractAlignedPriceVolume(btcChart) : [];
  const qqqPrices = qqqChart ? extractClosePrices(qqqChart) : [];
  const xlpPrices = xlpChart ? extractClosePrices(xlpChart) : [];
  const jpyRoc30 = rateOfChange(jpyPrices, 30);
  const liquidityStatus = jpyRoc30 !== null ? jpyRoc30 < -2 ? "SQUEEZE" : "NORMAL" : "UNKNOWN";
  const btcReturn5 = rateOfChange(btcPrices, 5);
  const qqqReturn5 = rateOfChange(qqqPrices, 5);
  let flowStatus = "UNKNOWN";
  if (btcReturn5 !== null && qqqReturn5 !== null) {
    const gap = btcReturn5 - qqqReturn5;
    flowStatus = Math.abs(gap) > 5 ? "PASSIVE GAP" : "ALIGNED";
  }
  const qqqRoc20 = rateOfChange(qqqPrices, 20);
  const xlpRoc20 = rateOfChange(xlpPrices, 20);
  let regimeStatus = "UNKNOWN";
  if (qqqRoc20 !== null && xlpRoc20 !== null) {
    regimeStatus = qqqRoc20 > xlpRoc20 ? "RISK-ON" : "DEFENSIVE";
  }
  const btcSma50 = smaCalc(btcPrices, 50);
  const btcSma200 = smaCalc(btcPrices, 200);
  const btcCurrent = btcPrices.length > 0 ? btcPrices[btcPrices.length - 1] : null;
  let btcVwap = null;
  if (btcAligned.length >= 30) {
    const last30 = btcAligned.slice(-30);
    let sumPV = 0, sumV = 0;
    for (const { price, volume } of last30) {
      sumPV += price * volume;
      sumV += volume;
    }
    if (sumV > 0) btcVwap = +(sumPV / sumV).toFixed(0);
  }
  let trendStatus = "UNKNOWN";
  let mayerMultiple = null;
  if (btcCurrent && btcSma50) {
    const aboveSma = btcCurrent > btcSma50 * 1.02;
    const belowSma = btcCurrent < btcSma50 * 0.98;
    const aboveVwap = btcVwap ? btcCurrent > btcVwap : null;
    if (aboveSma && aboveVwap !== false) trendStatus = "BULLISH";
    else if (belowSma && aboveVwap !== true) trendStatus = "BEARISH";
    else trendStatus = "NEUTRAL";
  }
  if (btcCurrent && btcSma200) {
    mayerMultiple = +(btcCurrent / btcSma200).toFixed(2);
  }
  let hashStatus = "UNKNOWN";
  let hashChange = null;
  if (mempoolHash.status === "fulfilled") {
    const hr = mempoolHash.value?.hashrates || mempoolHash.value;
    if (Array.isArray(hr) && hr.length >= 2) {
      const recent = hr[hr.length - 1]?.avgHashrate || hr[hr.length - 1];
      const older = hr[0]?.avgHashrate || hr[0];
      if (recent && older && older > 0) {
        hashChange = +((recent - older) / older * 100).toFixed(1);
        hashStatus = hashChange > 3 ? "GROWING" : hashChange < -3 ? "DECLINING" : "STABLE";
      }
    }
  }
  let miningStatus = "UNKNOWN";
  if (btcCurrent && hashChange !== null) {
    miningStatus = btcCurrent > 6e4 ? "PROFITABLE" : btcCurrent > 4e4 ? "TIGHT" : "SQUEEZE";
  }
  let fgValue;
  let fgLabel = "UNKNOWN";
  let fgHistory = [];
  if (fearGreed.status === "fulfilled" && fearGreed.value?.data) {
    const data = fearGreed.value.data;
    const parsed = parseInt(data[0]?.value, 10);
    fgValue = Number.isFinite(parsed) ? parsed : void 0;
    fgLabel = data[0]?.value_classification || "UNKNOWN";
    fgHistory = data.slice(0, 30).map((d) => ({
      value: parseInt(d.value, 10),
      date: new Date(parseInt(d.timestamp, 10) * 1e3).toISOString().slice(0, 10)
    })).reverse();
  }
  const btcSparkline = btcPrices.slice(-30);
  const qqqSparkline = qqqPrices.slice(-30);
  const jpySparkline = jpyPrices.slice(-30);
  let bullishCount = 0;
  let totalCount = 0;
  const signalList = [
    { name: "Liquidity", status: liquidityStatus, bullish: liquidityStatus === "NORMAL" },
    { name: "Flow Structure", status: flowStatus, bullish: flowStatus === "ALIGNED" },
    { name: "Macro Regime", status: regimeStatus, bullish: regimeStatus === "RISK-ON" },
    { name: "Technical Trend", status: trendStatus, bullish: trendStatus === "BULLISH" },
    { name: "Hash Rate", status: hashStatus, bullish: hashStatus === "GROWING" },
    { name: "Mining Cost", status: miningStatus, bullish: miningStatus === "PROFITABLE" },
    { name: "Fear & Greed", status: fgLabel, bullish: fgValue !== void 0 && fgValue > 50 }
  ];
  for (const s of signalList) {
    if (s.status !== "UNKNOWN") {
      totalCount++;
      if (s.bullish) bullishCount++;
    }
  }
  const verdict = totalCount === 0 ? "UNKNOWN" : bullishCount / totalCount >= 0.57 ? "BUY" : "CASH";
  return {
    timestamp: (/* @__PURE__ */ new Date()).toISOString(),
    verdict,
    bullishCount,
    totalCount,
    signals: {
      liquidity: {
        status: liquidityStatus,
        value: jpyRoc30 !== null ? +jpyRoc30.toFixed(2) : void 0,
        sparkline: jpySparkline
      },
      flowStructure: {
        status: flowStatus,
        btcReturn5: btcReturn5 !== null ? +btcReturn5.toFixed(2) : void 0,
        qqqReturn5: qqqReturn5 !== null ? +qqqReturn5.toFixed(2) : void 0
      },
      macroRegime: {
        status: regimeStatus,
        qqqRoc20: qqqRoc20 !== null ? +qqqRoc20.toFixed(2) : void 0,
        xlpRoc20: xlpRoc20 !== null ? +xlpRoc20.toFixed(2) : void 0
      },
      technicalTrend: {
        status: trendStatus,
        btcPrice: btcCurrent ?? void 0,
        sma50: btcSma50 ? +btcSma50.toFixed(0) : void 0,
        sma200: btcSma200 ? +btcSma200.toFixed(0) : void 0,
        vwap30d: btcVwap ?? void 0,
        mayerMultiple: mayerMultiple ?? void 0,
        sparkline: btcSparkline
      },
      hashRate: {
        status: hashStatus,
        change30d: hashChange ?? void 0
      },
      miningCost: { status: miningStatus },
      fearGreed: {
        status: fgLabel,
        value: fgValue,
        history: fgHistory
      }
    },
    meta: { qqqSparkline },
    unavailable: false
  };
}
async function getMacroSignals(_ctx, _req) {
  const now = Date.now();
  if (macroSignalsCached && now - macroSignalsCacheTimestamp < MACRO_CACHE_TTL * 1e3) {
    return macroSignalsCached;
  }
  try {
    const result = await cachedFetchJson(REDIS_CACHE_KEY17, REDIS_CACHE_TTL17, async () => {
      const computed = await computeMacroSignals();
      return !computed.unavailable && computed.totalCount > 0 ? computed : null;
    });
    if (result && !result.unavailable && result.totalCount > 0) {
      macroSignalsCached = result;
      macroSignalsCacheTimestamp = now;
      return result;
    }
    const fallback = macroSignalsCached || buildFallbackResult();
    macroSignalsCached = fallback;
    macroSignalsCacheTimestamp = now;
    return fallback;
  } catch {
    const fallback = macroSignalsCached || buildFallbackResult();
    macroSignalsCached = fallback;
    macroSignalsCacheTimestamp = now;
    return fallback;
  }
}

// server/worldmonitor/economic/v1/get-energy-capacity.ts
var REDIS_CACHE_KEY18 = "economic:capacity:v1";
var REDIS_CACHE_TTL18 = 86400;
var DEFAULT_YEARS = 20;
var EIA_CAPACITY_SOURCES = [
  { code: "SUN", name: "Solar" },
  { code: "WND", name: "Wind" },
  { code: "COL", name: "Coal" }
];
var COAL_SUBTYPES = ["BIT", "SUB", "LIG", "RC"];
async function fetchCapacityForSource(sourceCode, apiKey, startYear) {
  const params = new URLSearchParams({
    api_key: apiKey,
    "data[]": "capability",
    frequency: "annual",
    "facets[energysourceid][]": sourceCode,
    "sort[0][column]": "period",
    "sort[0][direction]": "desc",
    length: "5000",
    start: String(startYear)
  });
  const url = `https://api.eia.gov/v2/electricity/state-electricity-profiles/capability/data/?${params}`;
  const response = await fetch(url, {
    headers: { Accept: "application/json", "User-Agent": CHROME_UA },
    signal: AbortSignal.timeout(15e3)
  });
  if (!response.ok) return /* @__PURE__ */ new Map();
  const data = await response.json();
  const rows = data.response?.data;
  if (!rows || rows.length === 0) return /* @__PURE__ */ new Map();
  const yearTotals = /* @__PURE__ */ new Map();
  for (const row of rows) {
    if (row.period == null || row.capability == null) continue;
    const year = parseInt(row.period, 10);
    if (isNaN(year)) continue;
    const mw = typeof row.capability === "number" ? row.capability : parseFloat(String(row.capability));
    if (!Number.isFinite(mw)) continue;
    yearTotals.set(year, (yearTotals.get(year) ?? 0) + mw);
  }
  return yearTotals;
}
async function fetchCoalCapacity(apiKey, startYear) {
  const colResult = await fetchCapacityForSource("COL", apiKey, startYear);
  if (colResult.size > 0) return colResult;
  const subResults = await Promise.all(
    COAL_SUBTYPES.map((code) => fetchCapacityForSource(code, apiKey, startYear))
  );
  const merged = /* @__PURE__ */ new Map();
  for (const subMap of subResults) {
    for (const [year, mw] of subMap) {
      merged.set(year, (merged.get(year) ?? 0) + mw);
    }
  }
  return merged;
}
async function getEnergyCapacity(_ctx, req) {
  try {
    const apiKey = process.env.EIA_API_KEY;
    if (!apiKey) return { series: [] };
    const years = req.years > 0 ? req.years : DEFAULT_YEARS;
    const currentYear = (/* @__PURE__ */ new Date()).getFullYear();
    const startYear = currentYear - years;
    const requestedSources = req.energySources.length > 0 ? EIA_CAPACITY_SOURCES.filter((s) => req.energySources.includes(s.code)) : EIA_CAPACITY_SOURCES;
    if (requestedSources.length === 0) return { series: [] };
    const sourceKey = requestedSources.map((s) => s.code).sort().join(",");
    const cacheKey2 = `${REDIS_CACHE_KEY18}:${sourceKey}:${years}`;
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL18, async () => {
      const seriesResults = [];
      for (const source of requestedSources) {
        try {
          const yearTotals = source.code === "COL" ? await fetchCoalCapacity(apiKey, startYear) : await fetchCapacityForSource(source.code, apiKey, startYear);
          const dataPoints = Array.from(yearTotals.entries()).sort(([a], [b]) => a - b).map(([year, mw]) => ({ year, capacityMw: mw }));
          seriesResults.push({
            energySource: source.code,
            name: source.name,
            data: dataPoints
          });
        } catch {
          seriesResults.push({
            energySource: source.code,
            name: source.name,
            data: []
          });
        }
      }
      const hasData = seriesResults.some((s) => s.data.length > 0);
      return hasData ? { series: seriesResults } : null;
    });
    return result || { series: [] };
  } catch {
    return { series: [] };
  }
}

// server/worldmonitor/economic/v1/_bis-shared.ts
var import_papaparse = __toESM(require_papaparse(), 1);
var BIS_BASE = "https://stats.bis.org/api/v1/data";
var BIS_COUNTRIES = {
  US: { name: "United States", centralBank: "Federal Reserve" },
  GB: { name: "United Kingdom", centralBank: "Bank of England" },
  JP: { name: "Japan", centralBank: "Bank of Japan" },
  XM: { name: "Euro Area", centralBank: "ECB" },
  CH: { name: "Switzerland", centralBank: "Swiss National Bank" },
  SG: { name: "Singapore", centralBank: "MAS" },
  IN: { name: "India", centralBank: "Reserve Bank of India" },
  AU: { name: "Australia", centralBank: "RBA" },
  CN: { name: "China", centralBank: "People's Bank of China" },
  CA: { name: "Canada", centralBank: "Bank of Canada" },
  KR: { name: "South Korea", centralBank: "Bank of Korea" },
  BR: { name: "Brazil", centralBank: "Banco Central do Brasil" }
};
var BIS_COUNTRY_KEYS = Object.keys(BIS_COUNTRIES).join("+");
async function fetchBisCSV(dataset, key, timeout = 12e3) {
  const separator = key.includes("?") ? "&" : "?";
  const url = `${BIS_BASE}/${dataset}/${key}${separator}format=csv`;
  const controller = new AbortController();
  const id = setTimeout(() => controller.abort(), timeout);
  try {
    const res = await fetch(url, {
      headers: { "User-Agent": CHROME_UA, Accept: "text/csv" },
      signal: controller.signal
    });
    if (!res.ok) throw new Error(`BIS HTTP ${res.status}`);
    return await res.text();
  } finally {
    clearTimeout(id);
  }
}
function parseBisCSV(csv) {
  const result = import_papaparse.default.parse(csv, {
    header: true,
    skipEmptyLines: true,
    dynamicTyping: false
    // keep as strings, parse numbers explicitly
  });
  if (result.errors.length > 0) {
    console.warn("[BIS] CSV parse errors:", result.errors.slice(0, 3));
    if (result.data.length === 0) return [];
  }
  return result.data;
}
function parseBisNumber(val) {
  if (!val || val === "." || val.trim() === "") return null;
  const n = Number(val);
  return Number.isFinite(n) ? n : null;
}

// server/worldmonitor/economic/v1/get-bis-policy-rates.ts
var REDIS_CACHE_KEY19 = "economic:bis:policy:v1";
var REDIS_CACHE_TTL19 = 21600;
async function getBisPolicyRates(_ctx, _req) {
  try {
    const result = await cachedFetchJson(REDIS_CACHE_KEY19, REDIS_CACHE_TTL19, async () => {
      const threeMonthsAgo = /* @__PURE__ */ new Date();
      threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
      const startPeriod = `${threeMonthsAgo.getFullYear()}-${String(threeMonthsAgo.getMonth() + 1).padStart(2, "0")}`;
      const csv = await fetchBisCSV("WS_CBPOL", `M.${BIS_COUNTRY_KEYS}?startPeriod=${startPeriod}&detail=dataonly`);
      const rows = parseBisCSV(csv);
      const byCountry = /* @__PURE__ */ new Map();
      for (const row of rows) {
        const cc = row["REF_AREA"] || row["Reference area"] || "";
        const date = row["TIME_PERIOD"] || row["Time period"] || "";
        const val = parseBisNumber(row["OBS_VALUE"] || row["Observation value"]);
        if (!cc || !date || val === null) continue;
        if (!byCountry.has(cc)) byCountry.set(cc, []);
        byCountry.get(cc).push({ date, value: val });
      }
      const rates = [];
      for (const [cc, obs] of byCountry) {
        const info = BIS_COUNTRIES[cc];
        if (!info) continue;
        obs.sort((a, b) => a.date.localeCompare(b.date));
        const latest = obs[obs.length - 1];
        const previous = obs.length >= 2 ? obs[obs.length - 2] : void 0;
        if (latest) {
          rates.push({
            countryCode: cc,
            countryName: info.name,
            rate: latest.value,
            previousRate: previous?.value ?? latest.value,
            date: latest.date,
            centralBank: info.centralBank
          });
        }
      }
      return rates.length > 0 ? { rates } : null;
    });
    return result || { rates: [] };
  } catch (e) {
    console.error("[BIS] Policy rates fetch failed:", e);
    return { rates: [] };
  }
}

// server/worldmonitor/economic/v1/get-bis-exchange-rates.ts
var REDIS_CACHE_KEY20 = "economic:bis:eer:v1";
var REDIS_CACHE_TTL20 = 21600;
async function getBisExchangeRates(_ctx, _req) {
  try {
    const result = await cachedFetchJson(REDIS_CACHE_KEY20, REDIS_CACHE_TTL20, async () => {
      const threeMonthsAgo = /* @__PURE__ */ new Date();
      threeMonthsAgo.setMonth(threeMonthsAgo.getMonth() - 3);
      const startPeriod = `${threeMonthsAgo.getFullYear()}-${String(threeMonthsAgo.getMonth() + 1).padStart(2, "0")}`;
      const csv = await fetchBisCSV("WS_EER", `M.R.B.${BIS_COUNTRY_KEYS}?startPeriod=${startPeriod}&detail=dataonly`);
      const rows = parseBisCSV(csv);
      const byCountry = /* @__PURE__ */ new Map();
      for (const row of rows) {
        const cc = row["REF_AREA"] || row["Reference area"] || "";
        const date = row["TIME_PERIOD"] || row["Time period"] || "";
        const val = parseBisNumber(row["OBS_VALUE"] || row["Observation value"]);
        if (!cc || !date || val === null) continue;
        if (!byCountry.has(cc)) byCountry.set(cc, []);
        byCountry.get(cc).push({ date, value: val });
      }
      const rates = [];
      for (const [cc, obs] of byCountry) {
        const info = BIS_COUNTRIES[cc];
        if (!info) continue;
        obs.sort((a, b) => a.date.localeCompare(b.date));
        const latest = obs[obs.length - 1];
        const prev = obs.length >= 2 ? obs[obs.length - 2] : void 0;
        if (latest) {
          const realChange = prev ? Math.round((latest.value - prev.value) / prev.value * 1e3) / 10 : 0;
          rates.push({
            countryCode: cc,
            countryName: info.name,
            realEer: Math.round(latest.value * 100) / 100,
            nominalEer: 0,
            realChange,
            date: latest.date
          });
        }
      }
      return rates.length > 0 ? { rates } : null;
    });
    return result || { rates: [] };
  } catch (e) {
    console.error("[BIS] Exchange rates fetch failed:", e);
    return { rates: [] };
  }
}

// server/worldmonitor/economic/v1/get-bis-credit.ts
var REDIS_CACHE_KEY21 = "economic:bis:credit:v1";
var REDIS_CACHE_TTL21 = 43200;
async function getBisCredit(_ctx, _req) {
  try {
    const result = await cachedFetchJson(REDIS_CACHE_KEY21, REDIS_CACHE_TTL21, async () => {
      const twoYearsAgo = /* @__PURE__ */ new Date();
      twoYearsAgo.setFullYear(twoYearsAgo.getFullYear() - 2);
      const startPeriod = `${twoYearsAgo.getFullYear()}-Q1`;
      const csv = await fetchBisCSV("WS_TC", `Q.${BIS_COUNTRY_KEYS}.C.A.M.770.A?startPeriod=${startPeriod}&detail=dataonly`);
      const rows = parseBisCSV(csv);
      const byCountry = /* @__PURE__ */ new Map();
      for (const row of rows) {
        const cc = row["REF_AREA"] || row["Reference area"] || "";
        const date = row["TIME_PERIOD"] || row["Time period"] || "";
        const val = parseBisNumber(row["OBS_VALUE"] || row["Observation value"]);
        if (!cc || !date || val === null) continue;
        if (!byCountry.has(cc)) byCountry.set(cc, []);
        byCountry.get(cc).push({ date, value: val });
      }
      const entries = [];
      for (const [cc, obs] of byCountry) {
        const info = BIS_COUNTRIES[cc];
        if (!info) continue;
        obs.sort((a, b) => a.date.localeCompare(b.date));
        const latest = obs[obs.length - 1];
        const previous = obs.length >= 2 ? obs[obs.length - 2] : void 0;
        if (latest) {
          entries.push({
            countryCode: cc,
            countryName: info.name,
            creditGdpRatio: Math.round(latest.value * 10) / 10,
            previousRatio: previous ? Math.round(previous.value * 10) / 10 : Math.round(latest.value * 10) / 10,
            date: latest.date
          });
        }
      }
      return entries.length > 0 ? { entries } : null;
    });
    return result || { entries: [] };
  } catch (e) {
    console.error("[BIS] Credit-to-GDP fetch failed:", e);
    return { entries: [] };
  }
}

// server/worldmonitor/economic/v1/handler.ts
var economicHandler = {
  getFredSeries,
  listWorldBankIndicators,
  getEnergyPrices,
  getMacroSignals,
  getEnergyCapacity,
  getBisPolicyRates,
  getBisExchangeRates,
  getBisCredit
};

// src/generated/server/worldmonitor/infrastructure/v1/service_server.ts
var ValidationError13 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createInfrastructureServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/infrastructure/v1/list-internet-outages",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            start: Number(params.get("start") ?? "0"),
            end: Number(params.get("end") ?? "0"),
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            country: params.get("country") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listInternetOutages", body);
            if (bodyViolations) {
              throw new ValidationError13(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listInternetOutages(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError13) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/infrastructure/v1/list-service-statuses",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            status: params.get("status") ?? "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED"
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listServiceStatuses", body);
            if (bodyViolations) {
              throw new ValidationError13(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listServiceStatuses(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError13) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/infrastructure/v1/get-temporal-baseline",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            type: params.get("type") ?? "",
            region: params.get("region") ?? "",
            count: Number(params.get("count") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getTemporalBaseline", body);
            if (bodyViolations) {
              throw new ValidationError13(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getTemporalBaseline(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError13) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "POST",
      path: "/api/infrastructure/v1/record-baseline-snapshot",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = await req.json();
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("recordBaselineSnapshot", body);
            if (bodyViolations) {
              throw new ValidationError13(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.recordBaselineSnapshot(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError13) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/infrastructure/v1/get-cable-health",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getCableHealth(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError13) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/infrastructure/v1/_shared.ts
var UPSTREAM_TIMEOUT_MS2 = 1e4;
var BASELINE_TTL = 7776e3;
var MIN_SAMPLES = 10;
var Z_THRESHOLD_LOW = 1.5;
var Z_THRESHOLD_MEDIUM = 2;
var Z_THRESHOLD_HIGH = 3;
var VALID_BASELINE_TYPES = [
  "military_flights",
  "vessels",
  "protests",
  "news",
  "ais_gaps",
  "satellite_fires"
];
function makeBaselineKey(type, region, weekday, month) {
  return `baseline:${type}:${region}:${weekday}:${month}`;
}
function getBaselineSeverity(zScore) {
  if (zScore >= Z_THRESHOLD_HIGH) return "critical";
  if (zScore >= Z_THRESHOLD_MEDIUM) return "high";
  if (zScore >= Z_THRESHOLD_LOW) return "medium";
  return "normal";
}
async function mgetJson(keys) {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) return keys.map(() => null);
  try {
    const resp = await fetch(`${url}`, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(["MGET", ...keys]),
      signal: AbortSignal.timeout(5e3)
    });
    if (!resp.ok) return keys.map(() => null);
    const data = await resp.json();
    return (data.result || []).map((v) => v ? JSON.parse(v) : null);
  } catch {
    return keys.map(() => null);
  }
}

// server/worldmonitor/infrastructure/v1/get-cable-health.ts
var CACHE_KEY3 = "cable-health-v1";
var CACHE_TTL3 = 600;
var fallbackCache2 = null;
var CABLE_KEYWORDS = [
  "CABLE",
  "CABLESHIP",
  "CABLE SHIP",
  "CABLE LAYING",
  "CABLE OPERATIONS",
  "SUBMARINE CABLE",
  "UNDERSEA CABLE",
  "FIBER OPTIC",
  "TELECOMMUNICATIONS CABLE"
];
var FAULT_KEYWORDS = /FAULT|BREAK|CUT|DAMAGE|SEVERED|RUPTURE|OUTAGE|FAILURE/i;
var SHIP_PATTERNS = [
  /CABLESHIP\s+([A-Z][A-Z0-9\s\-']+)/i,
  /CABLE\s+SHIP\s+([A-Z][A-Z0-9\s\-']+)/i,
  /CS\s+([A-Z][A-Z0-9\s\-']+)/i,
  /M\/V\s+([A-Z][A-Z0-9\s\-']+)/i
];
var ON_STATION_RE = /ON STATION|OPERATIONS IN PROGRESS|LAYING|REPAIRING|WORKING|COMMENCED/i;
var CABLE_NAME_MAP = {
  "MAREA": "marea",
  "GRACE HOPPER": "grace_hopper",
  "HAVFRUE": "havfrue",
  "FASTER": "faster",
  "SOUTHERN CROSS": "southern_cross",
  "CURIE": "curie",
  "SEA-ME-WE": "seamewe6",
  "SEAMEWE": "seamewe6",
  "SMW6": "seamewe6",
  "FLAG": "flag",
  "2AFRICA": "2africa",
  "WACS": "wacs",
  "EASSY": "eassy",
  "SAM-1": "sam1",
  "SAM1": "sam1",
  "ELLALINK": "ellalink",
  "ELLA LINK": "ellalink",
  "APG": "apg",
  "INDIGO": "indigo",
  "SJC": "sjc",
  "FARICE": "farice",
  "FALCON": "falcon"
};
var CABLE_LANDINGS = {
  marea: [[36.85, -75.98], [43.26, -2.93]],
  grace_hopper: [[40.57, -73.97], [50.83, -4.55], [43.26, -2.93]],
  havfrue: [[40.22, -74.01], [58.15, 8], [55.56, 8.13]],
  faster: [[43.37, -124.22], [34.95, 139.95], [34.32, 136.85]],
  southern_cross: [[-33.87, 151.21], [-36.85, 174.76], [33.74, -118.27]],
  curie: [[33.74, -118.27], [-33.05, -71.62]],
  seamewe6: [[1.35, 103.82], [19.08, 72.88], [25.13, 56.34], [21.49, 39.19], [29.97, 32.55], [43.3, 5.37]],
  flag: [[50.04, -5.66], [31.2, 29.92], [25.2, 55.27], [19.08, 72.88], [1.35, 103.82], [35.69, 139.69]],
  "2africa": [[50.83, -4.55], [38.72, -9.14], [14.69, -17.44], [6.52, 3.38], [-33.93, 18.42], [-4.04, 39.67], [21.49, 39.19], [31.26, 32.3]],
  wacs: [[-33.93, 18.42], [6.52, 3.38], [14.69, -17.44], [38.72, -9.14], [51.51, -0.13]],
  eassy: [[-29.85, 31.02], [-25.97, 32.58], [-6.8, 39.28], [-4.04, 39.67], [11.59, 43.15]],
  sam1: [[-22.91, -43.17], [-34.6, -58.38], [26.36, -80.08]],
  ellalink: [[38.72, -9.14], [-3.72, -38.52]],
  apg: [[35.69, 139.69], [25.15, 121.44], [22.29, 114.17], [1.35, 103.82]],
  indigo: [[-31.95, 115.86], [1.35, 103.82], [-6.21, 106.85]],
  sjc: [[35.69, 139.69], [36.07, 120.32], [1.35, 103.82], [22.29, 114.17]],
  farice: [[64.13, -21.9], [62.01, -6.77], [55.95, -3.19]],
  falcon: [[25.13, 56.34], [23.59, 58.38], [26.23, 50.59], [29.38, 47.98]]
};
async function fetchNgaWarnings2() {
  try {
    const res = await fetch(
      "https://msi.nga.mil/api/publications/broadcast-warn?output=json&status=A",
      { headers: { "User-Agent": CHROME_UA }, signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS2) }
    );
    if (!res.ok) return [];
    const data = await res.json();
    return Array.isArray(data) ? data : data?.warnings ?? [];
  } catch {
    return [];
  }
}
function isCableRelated(text) {
  const upper = text.toUpperCase();
  return CABLE_KEYWORDS.some((kw) => upper.includes(kw));
}
function parseCoordinates(text) {
  const coords = [];
  const dms = /(\d{1,3})-(\d{1,2}(?:\.\d+)?)\s*([NS])\s+(\d{1,3})-(\d{1,2}(?:\.\d+)?)\s*([EW])/gi;
  let m;
  while ((m = dms.exec(text)) !== null) {
    let lat = parseInt(m[1], 10) + parseFloat(m[2]) / 60;
    let lon = parseInt(m[4], 10) + parseFloat(m[5]) / 60;
    if (m[3].toUpperCase() === "S") lat = -lat;
    if (m[6].toUpperCase() === "W") lon = -lon;
    if (lat >= -90 && lat <= 90 && lon >= -180 && lon <= 180) coords.push([lat, lon]);
  }
  return coords;
}
function matchCableByName(text) {
  const upper = text.toUpperCase();
  for (const [name, id] of Object.entries(CABLE_NAME_MAP)) {
    if (upper.includes(name)) return id;
  }
  return null;
}
function findNearestCable(lat, lon) {
  let bestId = null;
  let bestDist = Infinity;
  const MAX_DIST_KM = 555;
  const cosLat = Math.cos(lat * Math.PI / 180);
  for (const [cableId, landings] of Object.entries(CABLE_LANDINGS)) {
    for (const [lLat, lLon] of landings) {
      const dLat = (lat - lLat) * 111;
      const dLon = (lon - lLon) * 111 * cosLat;
      const distKm = Math.sqrt(dLat ** 2 + dLon ** 2);
      if (distKm < bestDist && distKm < MAX_DIST_KM) {
        bestDist = distKm;
        bestId = cableId;
      }
    }
  }
  return bestId ? { cableId: bestId, distanceKm: bestDist } : null;
}
var MONTH_MAP = {
  JAN: 0,
  FEB: 1,
  MAR: 2,
  APR: 3,
  MAY: 4,
  JUN: 5,
  JUL: 6,
  AUG: 7,
  SEP: 8,
  OCT: 9,
  NOV: 10,
  DEC: 11
};
function parseIssueDate(dateStr) {
  const m = dateStr?.match(/(\d{2})(\d{4})Z\s+([A-Z]{3})\s+(\d{4})/i);
  if (!m) return 0;
  const d = new Date(Date.UTC(
    parseInt(m[4], 10),
    MONTH_MAP[m[3].toUpperCase()] ?? 0,
    parseInt(m[1], 10),
    parseInt(m[2].slice(0, 2), 10),
    parseInt(m[2].slice(2, 4), 10)
  ));
  return Number.isNaN(d.getTime()) ? 0 : d.getTime();
}
function hasShipName(text) {
  return SHIP_PATTERNS.some((pat) => pat.test(text));
}
function processNgaSignals(warnings) {
  const signals = [];
  const cableWarnings = warnings.filter((w) => isCableRelated(w.text || ""));
  for (const warning of cableWarnings) {
    const text = warning.text || "";
    const ts = parseIssueDate(warning.issueDate);
    const coords = parseCoordinates(text);
    let cableId = matchCableByName(text);
    let joinMethod = "name";
    let distanceKm = 0;
    if (!cableId && coords.length > 0) {
      const centLat = coords.reduce((s, c) => s + c[0], 0) / coords.length;
      const centLon = coords.reduce((s, c) => s + c[1], 0) / coords.length;
      const nearest = findNearestCable(centLat, centLon);
      if (nearest) {
        cableId = nearest.cableId;
        joinMethod = "geometry";
        distanceKm = Math.round(nearest.distanceKm);
      }
    }
    if (!cableId) continue;
    const isFault = FAULT_KEYWORDS.test(text);
    const isRepairShip = hasShipName(text);
    const isOnStation = ON_STATION_RE.test(text);
    const summaryText = text.slice(0, 150) + (text.length > 150 ? "..." : "");
    if (isFault) {
      signals.push({
        cableId,
        ts,
        severity: 1,
        confidence: joinMethod === "name" ? 0.9 : Math.max(0.4, 0.8 - distanceKm / 500),
        ttlSeconds: 5 * 86400,
        kind: "operator_fault",
        evidence: [{ source: "NGA", summary: `Fault/damage reported: ${summaryText}`, ts }]
      });
    } else {
      signals.push({
        cableId,
        ts,
        severity: 0.6,
        confidence: joinMethod === "name" ? 0.8 : Math.max(0.3, 0.7 - distanceKm / 500),
        ttlSeconds: 3 * 86400,
        kind: "cable_advisory",
        evidence: [{ source: "NGA", summary: `Cable advisory: ${summaryText}`, ts }]
      });
    }
    if (isRepairShip) {
      signals.push({
        cableId,
        ts,
        severity: isOnStation ? 0.8 : 0.5,
        confidence: isOnStation ? 0.85 : 0.6,
        ttlSeconds: isOnStation ? 24 * 3600 : 12 * 3600,
        kind: "repair_activity",
        evidence: [{
          source: "NGA",
          summary: isOnStation ? `Cable repair vessel on station: ${summaryText}` : `Cable ship in area: ${summaryText}`,
          ts
        }]
      });
    }
  }
  return signals;
}
function computeHealthMap(signals) {
  const now = Date.now();
  const byCable = {};
  for (const sig of signals) {
    if (!byCable[sig.cableId]) byCable[sig.cableId] = [];
    byCable[sig.cableId].push(sig);
  }
  const healthMap = {};
  for (const [cableId, cableSignals] of Object.entries(byCable)) {
    const effectiveSignals = [];
    for (const sig of cableSignals) {
      const ageMs = now - sig.ts;
      const ageSec = Math.max(0, ageMs / 1e3);
      const recencyWeight = Math.max(0, Math.min(1, 1 - ageSec / sig.ttlSeconds));
      if (recencyWeight <= 0) continue;
      const effective = sig.severity * sig.confidence * recencyWeight;
      effectiveSignals.push({ ...sig, effective, recencyWeight });
    }
    if (effectiveSignals.length === 0) continue;
    effectiveSignals.sort((a, b) => b.effective - a.effective);
    const topScore = effectiveSignals[0].effective;
    const topConfidence = effectiveSignals[0].confidence * effectiveSignals[0].recencyWeight;
    const hasOperatorFault = effectiveSignals.some(
      (s) => s.kind === "operator_fault" && s.effective >= 0.5
    );
    const hasRepairActivity = effectiveSignals.some(
      (s) => s.kind === "repair_activity" && s.effective >= 0.4
    );
    let status;
    if (topScore >= 0.8 && hasOperatorFault) {
      status = "CABLE_HEALTH_STATUS_FAULT";
    } else if (topScore >= 0.8 && hasRepairActivity) {
      status = "CABLE_HEALTH_STATUS_DEGRADED";
    } else if (topScore >= 0.5) {
      status = "CABLE_HEALTH_STATUS_DEGRADED";
    } else {
      status = "CABLE_HEALTH_STATUS_OK";
    }
    const evidence = effectiveSignals.slice(0, 3).flatMap((s) => s.evidence).slice(0, 3);
    const lastUpdated = effectiveSignals.map((s) => s.ts).sort((a, b) => b - a)[0];
    healthMap[cableId] = {
      status,
      score: Math.round(topScore * 100) / 100,
      confidence: Math.round(topConfidence * 100) / 100,
      lastUpdated,
      evidence
    };
  }
  return healthMap;
}
async function getCableHealth(_ctx, _req) {
  try {
    const result = await cachedFetchJson(CACHE_KEY3, CACHE_TTL3, async () => {
      const ngaData = await fetchNgaWarnings2();
      const signals = processNgaSignals(ngaData);
      const cables = computeHealthMap(signals);
      const response = {
        generatedAt: Date.now(),
        cables
      };
      return response;
    });
    if (result) {
      fallbackCache2 = result;
      return result;
    }
    return fallbackCache2 || { generatedAt: Date.now(), cables: {} };
  } catch {
    if (fallbackCache2) return fallbackCache2;
    return { generatedAt: Date.now(), cables: {} };
  }
}

// server/worldmonitor/infrastructure/v1/list-internet-outages.ts
var REDIS_CACHE_KEY22 = "infra:outages:v1";
var REDIS_CACHE_TTL22 = 1800;
var fallbackOutagesCache = null;
var CLOUDFLARE_RADAR_URL = "https://api.cloudflare.com/client/v4/radar/annotations/outages";
var COUNTRY_COORDS = {
  AF: [33.94, 67.71],
  AL: [41.15, 20.17],
  DZ: [28.03, 1.66],
  AO: [-11.2, 17.87],
  AR: [-38.42, -63.62],
  AM: [40.07, 45.04],
  AU: [-25.27, 133.78],
  AT: [47.52, 14.55],
  AZ: [40.14, 47.58],
  BH: [26.07, 50.56],
  BD: [23.69, 90.36],
  BY: [53.71, 27.95],
  BE: [50.5, 4.47],
  BJ: [9.31, 2.32],
  BO: [-16.29, -63.59],
  BA: [43.92, 17.68],
  BW: [-22.33, 24.68],
  BR: [-14.24, -51.93],
  BG: [42.73, 25.49],
  BF: [12.24, -1.56],
  BI: [-3.37, 29.92],
  KH: [12.57, 104.99],
  CM: [7.37, 12.35],
  CA: [56.13, -106.35],
  CF: [6.61, 20.94],
  TD: [15.45, 18.73],
  CL: [-35.68, -71.54],
  CN: [35.86, 104.2],
  CO: [4.57, -74.3],
  CG: [-0.23, 15.83],
  CD: [-4.04, 21.76],
  CR: [9.75, -83.75],
  HR: [45.1, 15.2],
  CU: [21.52, -77.78],
  CY: [35.13, 33.43],
  CZ: [49.82, 15.47],
  DK: [56.26, 9.5],
  DJ: [11.83, 42.59],
  EC: [-1.83, -78.18],
  EG: [26.82, 30.8],
  SV: [13.79, -88.9],
  ER: [15.18, 39.78],
  EE: [58.6, 25.01],
  ET: [9.15, 40.49],
  FI: [61.92, 25.75],
  FR: [46.23, 2.21],
  GA: [-0.8, 11.61],
  GM: [13.44, -15.31],
  GE: [42.32, 43.36],
  DE: [51.17, 10.45],
  GH: [7.95, -1.02],
  GR: [39.07, 21.82],
  GT: [15.78, -90.23],
  GN: [9.95, -9.7],
  HT: [18.97, -72.29],
  HN: [15.2, -86.24],
  HK: [22.32, 114.17],
  HU: [47.16, 19.5],
  IN: [20.59, 78.96],
  ID: [-0.79, 113.92],
  IR: [32.43, 53.69],
  IQ: [33.22, 43.68],
  IE: [53.14, -7.69],
  IL: [31.05, 34.85],
  IT: [41.87, 12.57],
  CI: [7.54, -5.55],
  JP: [36.2, 138.25],
  JO: [30.59, 36.24],
  KZ: [48.02, 66.92],
  KE: [-0.02, 37.91],
  KW: [29.31, 47.48],
  KG: [41.2, 74.77],
  LA: [19.86, 102.5],
  LV: [56.88, 24.6],
  LB: [33.85, 35.86],
  LY: [26.34, 17.23],
  LT: [55.17, 23.88],
  LU: [49.82, 6.13],
  MG: [-18.77, 46.87],
  MW: [-13.25, 34.3],
  MY: [4.21, 101.98],
  ML: [17.57, -4],
  MR: [21.01, -10.94],
  MX: [23.63, -102.55],
  MD: [47.41, 28.37],
  MN: [46.86, 103.85],
  MA: [31.79, -7.09],
  MZ: [-18.67, 35.53],
  MM: [21.92, 95.96],
  NA: [-22.96, 18.49],
  NP: [28.39, 84.12],
  NL: [52.13, 5.29],
  NZ: [-40.9, 174.89],
  NI: [12.87, -85.21],
  NE: [17.61, 8.08],
  NG: [9.08, 8.68],
  KP: [40.34, 127.51],
  NO: [60.47, 8.47],
  OM: [21.47, 55.98],
  PK: [30.38, 69.35],
  PS: [31.95, 35.23],
  PA: [8.54, -80.78],
  PG: [-6.32, 143.96],
  PY: [-23.44, -58.44],
  PE: [-9.19, -75.02],
  PH: [12.88, 121.77],
  PL: [51.92, 19.15],
  PT: [39.4, -8.22],
  QA: [25.35, 51.18],
  RO: [45.94, 24.97],
  RU: [61.52, 105.32],
  RW: [-1.94, 29.87],
  SA: [23.89, 45.08],
  SN: [14.5, -14.45],
  RS: [44.02, 21.01],
  SL: [8.46, -11.78],
  SG: [1.35, 103.82],
  SK: [48.67, 19.7],
  SI: [46.15, 14.99],
  SO: [5.15, 46.2],
  ZA: [-30.56, 22.94],
  KR: [35.91, 127.77],
  SS: [6.88, 31.31],
  ES: [40.46, -3.75],
  LK: [7.87, 80.77],
  SD: [12.86, 30.22],
  SE: [60.13, 18.64],
  CH: [46.82, 8.23],
  SY: [34.8, 38.997],
  TW: [23.7, 120.96],
  TJ: [38.86, 71.28],
  TZ: [-6.37, 34.89],
  TH: [15.87, 100.99],
  TG: [8.62, 0.82],
  TT: [10.69, -61.22],
  TN: [33.89, 9.54],
  TR: [38.96, 35.24],
  TM: [38.97, 59.56],
  UG: [1.37, 32.29],
  UA: [48.38, 31.17],
  AE: [23.42, 53.85],
  GB: [55.38, -3.44],
  US: [37.09, -95.71],
  UY: [-32.52, -55.77],
  UZ: [41.38, 64.59],
  VE: [6.42, -66.59],
  VN: [14.06, 108.28],
  YE: [15.55, 48.52],
  ZM: [-13.13, 27.85],
  ZW: [-19.02, 29.15]
};
function mapOutageSeverity(outageType) {
  if (outageType === "NATIONWIDE") return "OUTAGE_SEVERITY_TOTAL";
  if (outageType === "REGIONAL") return "OUTAGE_SEVERITY_MAJOR";
  return "OUTAGE_SEVERITY_PARTIAL";
}
function toEpochMs2(value) {
  if (!value) return 0;
  const d = new Date(value);
  return Number.isNaN(d.getTime()) ? 0 : d.getTime();
}
function filterOutages(outages, req) {
  let filtered = outages;
  if (req.country) {
    const target = req.country.toLowerCase();
    filtered = filtered.filter((o) => o.country.toLowerCase().includes(target));
  }
  if (req.start) {
    filtered = filtered.filter((o) => o.detectedAt >= req.start);
  }
  if (req.end) {
    filtered = filtered.filter((o) => o.detectedAt <= req.end);
  }
  return filtered;
}
async function listInternetOutages(_ctx, req) {
  try {
    const result = await cachedFetchJson(REDIS_CACHE_KEY22, REDIS_CACHE_TTL22, async () => {
      const token = process.env.CLOUDFLARE_API_TOKEN;
      if (!token) return null;
      const response = await fetch(
        `${CLOUDFLARE_RADAR_URL}?dateRange=7d&limit=50`,
        {
          headers: { Authorization: `Bearer ${token}`, "User-Agent": CHROME_UA },
          signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS2)
        }
      );
      if (!response.ok) return null;
      const data = await response.json();
      if (data.configured === false || !data.success || data.errors?.length) return null;
      const outages = [];
      for (const raw of data.result?.annotations || []) {
        if (!raw.locations?.length) continue;
        const countryCode = raw.locations[0];
        if (!countryCode) continue;
        const coords = COUNTRY_COORDS[countryCode];
        if (!coords) continue;
        const countryName = raw.locationsDetails?.[0]?.name ?? countryCode;
        const categories = ["Cloudflare Radar"];
        if (raw.outage?.outageCause) categories.push(raw.outage.outageCause.replace(/_/g, " "));
        if (raw.outage?.outageType) categories.push(raw.outage.outageType);
        for (const asn of raw.asnsDetails?.slice(0, 2) || []) {
          if (asn.name) categories.push(asn.name);
        }
        outages.push({
          id: `cf-${raw.id}`,
          title: raw.scope ? `${raw.scope} outage in ${countryName}` : `Internet disruption in ${countryName}`,
          link: raw.linkedUrl || "https://radar.cloudflare.com/outage-center",
          description: raw.description,
          detectedAt: toEpochMs2(raw.startDate),
          country: countryName,
          region: "",
          location: { latitude: coords[0], longitude: coords[1] },
          severity: mapOutageSeverity(raw.outage?.outageType),
          categories,
          cause: raw.outage?.outageCause || "",
          outageType: raw.outage?.outageType || "",
          endedAt: toEpochMs2(raw.endDate)
        });
      }
      return outages.length > 0 ? { outages, pagination: void 0 } : null;
    });
    if (result) fallbackOutagesCache = { data: result, ts: Date.now() };
    const effective = result || fallbackOutagesCache?.data;
    return { outages: filterOutages(effective?.outages || [], req), pagination: void 0 };
  } catch {
    const stale = fallbackOutagesCache?.data?.outages || [];
    return { outages: filterOutages(stale, req), pagination: void 0 };
  }
}

// server/worldmonitor/infrastructure/v1/list-service-statuses.ts
var SERVICES = [
  // Cloud Providers
  { id: "aws", name: "AWS", statusPage: "https://health.aws.amazon.com/health/status", customParser: "aws", category: "cloud" },
  { id: "azure", name: "Azure", statusPage: "https://azure.status.microsoft/en-us/status/feed/", customParser: "rss", category: "cloud" },
  { id: "gcp", name: "Google Cloud", statusPage: "https://status.cloud.google.com/incidents.json", customParser: "gcp", category: "cloud" },
  { id: "cloudflare", name: "Cloudflare", statusPage: "https://www.cloudflarestatus.com/api/v2/status.json", category: "cloud" },
  { id: "vercel", name: "Vercel", statusPage: "https://www.vercel-status.com/api/v2/status.json", category: "cloud" },
  { id: "netlify", name: "Netlify", statusPage: "https://www.netlifystatus.com/api/v2/status.json", category: "cloud" },
  { id: "digitalocean", name: "DigitalOcean", statusPage: "https://status.digitalocean.com/api/v2/status.json", category: "cloud" },
  { id: "render", name: "Render", statusPage: "https://status.render.com/api/v2/status.json", category: "cloud" },
  { id: "railway", name: "Railway", statusPage: "https://railway.instatus.com/summary.json", customParser: "instatus", category: "cloud" },
  // Developer Tools
  { id: "github", name: "GitHub", statusPage: "https://www.githubstatus.com/api/v2/status.json", category: "dev" },
  { id: "gitlab", name: "GitLab", statusPage: "https://status.gitlab.com/1.0/status/5b36dc6502d06804c08349f7", customParser: "statusio", category: "dev" },
  { id: "npm", name: "npm", statusPage: "https://status.npmjs.org/api/v2/status.json", category: "dev" },
  { id: "docker", name: "Docker Hub", statusPage: "https://www.dockerstatus.com/1.0/status/533c6539221ae15e3f000031", customParser: "statusio", category: "dev" },
  { id: "bitbucket", name: "Bitbucket", statusPage: "https://bitbucket.status.atlassian.com/api/v2/status.json", category: "dev" },
  { id: "circleci", name: "CircleCI", statusPage: "https://status.circleci.com/api/v2/status.json", category: "dev" },
  { id: "jira", name: "Jira", statusPage: "https://jira-software.status.atlassian.com/api/v2/status.json", category: "dev" },
  { id: "confluence", name: "Confluence", statusPage: "https://confluence.status.atlassian.com/api/v2/status.json", category: "dev" },
  { id: "linear", name: "Linear", statusPage: "https://linearstatus.com/api/v2/status.json", customParser: "incidentio", category: "dev" },
  // Communication
  { id: "slack", name: "Slack", statusPage: "https://slack-status.com/api/v2.0.0/current", customParser: "slack", category: "comm" },
  { id: "discord", name: "Discord", statusPage: "https://discordstatus.com/api/v2/status.json", category: "comm" },
  { id: "zoom", name: "Zoom", statusPage: "https://www.zoomstatus.com/api/v2/status.json", category: "comm" },
  { id: "notion", name: "Notion", statusPage: "https://www.notion-status.com/api/v2/status.json", category: "comm" },
  // AI Services
  { id: "openai", name: "OpenAI", statusPage: "https://status.openai.com/api/v2/status.json", customParser: "incidentio", category: "ai" },
  { id: "anthropic", name: "Anthropic", statusPage: "https://status.claude.com/api/v2/status.json", customParser: "incidentio", category: "ai" },
  { id: "replicate", name: "Replicate", statusPage: "https://www.replicatestatus.com/api/v2/status.json", customParser: "incidentio", category: "ai" },
  // SaaS
  { id: "stripe", name: "Stripe", statusPage: "https://status.stripe.com/current", customParser: "stripe", category: "saas" },
  { id: "twilio", name: "Twilio", statusPage: "https://status.twilio.com/api/v2/status.json", category: "saas" },
  { id: "datadog", name: "Datadog", statusPage: "https://status.datadoghq.com/api/v2/status.json", category: "saas" },
  { id: "sentry", name: "Sentry", statusPage: "https://status.sentry.io/api/v2/status.json", category: "saas" },
  { id: "supabase", name: "Supabase", statusPage: "https://status.supabase.com/api/v2/status.json", category: "saas" }
];
function normalizeToProtoStatus(raw) {
  if (!raw) return "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED";
  const val = raw.toLowerCase();
  if (val === "none" || val === "operational" || val.includes("all systems operational")) {
    return "SERVICE_OPERATIONAL_STATUS_OPERATIONAL";
  }
  if (val === "minor" || val === "degraded_performance" || val.includes("degraded")) {
    return "SERVICE_OPERATIONAL_STATUS_DEGRADED";
  }
  if (val === "partial_outage") {
    return "SERVICE_OPERATIONAL_STATUS_PARTIAL_OUTAGE";
  }
  if (val === "major" || val === "major_outage" || val === "critical" || val.includes("outage")) {
    return "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE";
  }
  if (val === "maintenance" || val.includes("maintenance")) {
    return "SERVICE_OPERATIONAL_STATUS_MAINTENANCE";
  }
  return "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED";
}
async function checkServiceStatus(service) {
  const now = Date.now();
  const base = {
    id: service.id,
    name: service.name,
    url: service.statusPage
  };
  const unknown = (desc) => ({
    ...base,
    status: "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED",
    description: desc,
    checkedAt: now,
    latencyMs: 0
  });
  try {
    const headers = {
      Accept: service.customParser === "rss" ? "application/xml, text/xml" : "application/json, text/plain, */*",
      "Accept-Language": "en-US,en;q=0.9",
      "Cache-Control": "no-cache"
    };
    if (service.customParser !== "incidentio") {
      headers["User-Agent"] = CHROME_UA;
    }
    const start = Date.now();
    const response = await fetch(service.statusPage, {
      headers,
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS2)
    });
    const latencyMs = Date.now() - start;
    if (!response.ok) {
      return { ...base, status: "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED", description: `HTTP ${response.status}`, checkedAt: now, latencyMs };
    }
    if (service.customParser === "gcp") {
      const data2 = await response.json();
      const active = Array.isArray(data2) ? data2.filter((i) => i.end === void 0 || new Date(i.end) > /* @__PURE__ */ new Date()) : [];
      if (active.length === 0) {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL", description: "All services operational", checkedAt: now, latencyMs };
      }
      const hasHigh = active.some((i) => i.severity === "high");
      return {
        ...base,
        status: hasHigh ? "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE" : "SERVICE_OPERATIONAL_STATUS_DEGRADED",
        description: `${active.length} active incident(s)`,
        checkedAt: now,
        latencyMs
      };
    }
    if (service.customParser === "aws") {
      return { ...base, status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL", description: "Status page reachable", checkedAt: now, latencyMs };
    }
    if (service.customParser === "rss") {
      const text2 = await response.text();
      const hasIncident = text2.includes("<item>") && (text2.includes("degradation") || text2.includes("outage") || text2.includes("incident"));
      return {
        ...base,
        status: hasIncident ? "SERVICE_OPERATIONAL_STATUS_DEGRADED" : "SERVICE_OPERATIONAL_STATUS_OPERATIONAL",
        description: hasIncident ? "Recent incidents reported" : "No recent incidents",
        checkedAt: now,
        latencyMs
      };
    }
    if (service.customParser === "instatus") {
      const data2 = await response.json();
      const pageStatus = data2.page?.status;
      if (pageStatus === "UP") {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL", description: "All systems operational", checkedAt: now, latencyMs };
      }
      if (pageStatus === "HASISSUES") {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_DEGRADED", description: "Some issues reported", checkedAt: now, latencyMs };
      }
      return unknown(pageStatus || "Unknown");
    }
    if (service.customParser === "statusio") {
      const data2 = await response.json();
      const overall = data2.result?.status_overall;
      const code = overall?.status_code;
      if (code === 100) {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL", description: overall.status || "All systems operational", checkedAt: now, latencyMs };
      }
      if (code >= 300 && code < 500) {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_DEGRADED", description: overall.status || "Degraded performance", checkedAt: now, latencyMs };
      }
      if (code >= 500) {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE", description: overall.status || "Service disruption", checkedAt: now, latencyMs };
      }
      return unknown(overall?.status || "Unknown status");
    }
    if (service.customParser === "slack") {
      const data2 = await response.json();
      if (data2.status === "ok") {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL", description: "All systems operational", checkedAt: now, latencyMs };
      }
      if (data2.status === "active" || data2.active_incidents?.length > 0) {
        const count = data2.active_incidents?.length || 1;
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_DEGRADED", description: `${count} active incident(s)`, checkedAt: now, latencyMs };
      }
      return unknown(data2.status || "Unknown");
    }
    if (service.customParser === "stripe") {
      const data2 = await response.json();
      if (data2.largestatus === "up") {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL", description: data2.message || "All systems operational", checkedAt: now, latencyMs };
      }
      if (data2.largestatus === "degraded") {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_DEGRADED", description: data2.message || "Degraded performance", checkedAt: now, latencyMs };
      }
      if (data2.largestatus === "down") {
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE", description: data2.message || "Service disruption", checkedAt: now, latencyMs };
      }
      return unknown(data2.message || "Unknown");
    }
    if (service.customParser === "incidentio") {
      const text2 = await response.text();
      if (text2.startsWith("<!") || text2.startsWith("<html")) {
        if (/All Systems Operational|fully operational|no issues/i.test(text2)) {
          return { ...base, status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL", description: "All systems operational", checkedAt: now, latencyMs };
        }
        if (/degraded|partial outage|experiencing issues/i.test(text2)) {
          return { ...base, status: "SERVICE_OPERATIONAL_STATUS_DEGRADED", description: "Some issues reported", checkedAt: now, latencyMs };
        }
        return unknown("Could not parse status");
      }
      try {
        const data2 = JSON.parse(text2);
        const indicator = data2.status?.indicator || "";
        const description = data2.status?.description || "";
        if (indicator === "none" || description.toLowerCase().includes("operational")) {
          return { ...base, status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL", description: description || "All systems operational", checkedAt: now, latencyMs };
        }
        if (indicator === "minor" || indicator === "maintenance") {
          return { ...base, status: "SERVICE_OPERATIONAL_STATUS_DEGRADED", description: description || "Minor issues", checkedAt: now, latencyMs };
        }
        if (indicator === "major" || indicator === "critical") {
          return { ...base, status: "SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE", description: description || "Major outage", checkedAt: now, latencyMs };
        }
        return { ...base, status: "SERVICE_OPERATIONAL_STATUS_OPERATIONAL", description: description || "Status OK", checkedAt: now, latencyMs };
      } catch {
        return unknown("Invalid response");
      }
    }
    const text = await response.text();
    if (text.startsWith("<!") || text.startsWith("<html")) {
      return unknown("Blocked by service");
    }
    let data;
    try {
      data = JSON.parse(text);
    } catch {
      return unknown("Invalid JSON response");
    }
    if (data.status?.indicator !== void 0) {
      return {
        ...base,
        status: normalizeToProtoStatus(data.status.indicator),
        description: data.status.description || "",
        checkedAt: now,
        latencyMs
      };
    }
    if (data.status?.status) {
      return {
        ...base,
        status: data.status.status === "ok" ? "SERVICE_OPERATIONAL_STATUS_OPERATIONAL" : "SERVICE_OPERATIONAL_STATUS_DEGRADED",
        description: data.status.description || "",
        checkedAt: now,
        latencyMs
      };
    }
    if (data.page && data.status) {
      return {
        ...base,
        status: normalizeToProtoStatus(data.status.indicator || data.status.description),
        description: data.status.description || "Status available",
        checkedAt: now,
        latencyMs
      };
    }
    return unknown("Unknown format");
  } catch {
    return unknown("Request failed");
  }
}
var INFRA_CACHE_KEY = "infra:service-statuses:v1";
var INFRA_CACHE_TTL = 1800;
var fallbackStatusesCache = null;
var STATUS_ORDER = {
  SERVICE_OPERATIONAL_STATUS_MAJOR_OUTAGE: 0,
  SERVICE_OPERATIONAL_STATUS_PARTIAL_OUTAGE: 1,
  SERVICE_OPERATIONAL_STATUS_DEGRADED: 2,
  SERVICE_OPERATIONAL_STATUS_MAINTENANCE: 3,
  SERVICE_OPERATIONAL_STATUS_UNSPECIFIED: 4,
  SERVICE_OPERATIONAL_STATUS_OPERATIONAL: 5
};
function filterAndSortStatuses(statuses, req) {
  let filtered = statuses;
  if (req.status && req.status !== "SERVICE_OPERATIONAL_STATUS_UNSPECIFIED") {
    filtered = statuses.filter((s) => s.status === req.status);
  }
  return [...filtered].sort((a, b) => (STATUS_ORDER[a.status] ?? 4) - (STATUS_ORDER[b.status] ?? 4));
}
async function listServiceStatuses(_ctx, req) {
  try {
    const results = await cachedFetchJson(INFRA_CACHE_KEY, INFRA_CACHE_TTL, async () => {
      const fresh = await Promise.all(SERVICES.map(checkServiceStatus));
      return fresh.length > 0 ? fresh : null;
    });
    const effective = results || fallbackStatusesCache?.data || [];
    if (results) fallbackStatusesCache = { data: results, ts: Date.now() };
    return { statuses: filterAndSortStatuses(effective, req) };
  } catch {
    return { statuses: filterAndSortStatuses(fallbackStatusesCache?.data || [], req) };
  }
}

// server/worldmonitor/infrastructure/v1/get-temporal-baseline.ts
async function getTemporalBaseline(_ctx, req) {
  try {
    const { type, count } = req;
    const region = req.region || "global";
    if (!type || !VALID_BASELINE_TYPES.includes(type) || typeof count !== "number" || isNaN(count)) {
      return {
        learning: false,
        sampleCount: 0,
        samplesNeeded: 0,
        error: "Missing or invalid params: type and count required"
      };
    }
    const now = /* @__PURE__ */ new Date();
    const weekday = now.getUTCDay();
    const month = now.getUTCMonth() + 1;
    const key = makeBaselineKey(type, region, weekday, month);
    const baseline = await getCachedJson(key);
    if (!baseline || baseline.sampleCount < MIN_SAMPLES) {
      return {
        learning: true,
        sampleCount: baseline?.sampleCount || 0,
        samplesNeeded: MIN_SAMPLES,
        error: ""
      };
    }
    const variance = Math.max(0, baseline.m2 / (baseline.sampleCount - 1));
    const stdDev = Math.sqrt(variance);
    const zScore = stdDev > 0 ? Math.abs((count - baseline.mean) / stdDev) : 0;
    const severity = getBaselineSeverity(zScore);
    const multiplier = baseline.mean > 0 ? Math.round(count / baseline.mean * 100) / 100 : count > 0 ? 999 : 1;
    return {
      anomaly: zScore >= Z_THRESHOLD_LOW ? {
        zScore: Math.round(zScore * 100) / 100,
        severity,
        multiplier
      } : void 0,
      baseline: {
        mean: Math.round(baseline.mean * 100) / 100,
        stdDev: Math.round(stdDev * 100) / 100,
        sampleCount: baseline.sampleCount
      },
      learning: false,
      sampleCount: baseline.sampleCount,
      samplesNeeded: MIN_SAMPLES,
      error: ""
    };
  } catch {
    return {
      learning: false,
      sampleCount: 0,
      samplesNeeded: 0,
      error: "Internal error"
    };
  }
}

// server/worldmonitor/infrastructure/v1/record-baseline-snapshot.ts
async function recordBaselineSnapshot(_ctx, req) {
  try {
    const updates = req.updates;
    if (!Array.isArray(updates) || updates.length === 0) {
      return { updated: 0, error: "Body must have updates array" };
    }
    const batch = updates.slice(0, 20);
    const now = /* @__PURE__ */ new Date();
    const weekday = now.getUTCDay();
    const month = now.getUTCMonth() + 1;
    const keys = batch.map((u) => makeBaselineKey(u.type, u.region || "global", weekday, month));
    const existing = await mgetJson(keys);
    const writes = [];
    for (let i = 0; i < batch.length; i++) {
      const { type, count } = batch[i];
      if (!VALID_BASELINE_TYPES.includes(type) || typeof count !== "number" || isNaN(count)) continue;
      const prev = existing[i] || { mean: 0, m2: 0, sampleCount: 0, lastUpdated: "" };
      const n = prev.sampleCount + 1;
      const delta = count - prev.mean;
      const newMean = prev.mean + delta / n;
      const delta2 = count - newMean;
      const newM2 = prev.m2 + delta * delta2;
      writes.push(setCachedJson(keys[i], {
        mean: newMean,
        m2: newM2,
        sampleCount: n,
        lastUpdated: now.toISOString()
      }, BASELINE_TTL));
    }
    if (writes.length > 0) {
      await Promise.all(writes);
    }
    return { updated: writes.length, error: "" };
  } catch {
    return { updated: 0, error: "Internal error" };
  }
}

// server/worldmonitor/infrastructure/v1/handler.ts
var infrastructureHandler = {
  getCableHealth,
  listInternetOutages,
  listServiceStatuses,
  getTemporalBaseline,
  recordBaselineSnapshot
};

// src/generated/server/worldmonitor/market/v1/service_server.ts
var ValidationError14 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createMarketServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/market/v1/list-market-quotes",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            symbols: params.getAll("symbols")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listMarketQuotes", body);
            if (bodyViolations) {
              throw new ValidationError14(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listMarketQuotes(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError14) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/market/v1/list-crypto-quotes",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            ids: params.getAll("ids")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listCryptoQuotes", body);
            if (bodyViolations) {
              throw new ValidationError14(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listCryptoQuotes(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError14) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/market/v1/list-commodity-quotes",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            symbols: params.getAll("symbols")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listCommodityQuotes", body);
            if (bodyViolations) {
              throw new ValidationError14(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listCommodityQuotes(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError14) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/market/v1/get-sector-summary",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            period: params.get("period") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getSectorSummary", body);
            if (bodyViolations) {
              throw new ValidationError14(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getSectorSummary(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError14) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/market/v1/list-stablecoin-markets",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            coins: params.getAll("coins")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listStablecoinMarkets", body);
            if (bodyViolations) {
              throw new ValidationError14(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listStablecoinMarkets(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError14) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/market/v1/list-etf-flows",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listEtfFlows(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError14) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/market/v1/get-country-stock-index",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            countryCode: params.get("country_code") ?? ""
          };
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getCountryStockIndex(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError14) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/market/v1/list-gulf-quotes",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listGulfQuotes(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError14) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/market/v1/_shared.ts
var UPSTREAM_TIMEOUT_MS3 = 1e4;
async function fetchYahooQuotesBatch(symbols) {
  const results = /* @__PURE__ */ new Map();
  let rateLimitHits = 0;
  for (let i = 0; i < symbols.length; i++) {
    const q = await fetchYahooQuote(symbols[i]);
    if (q) {
      results.set(symbols[i], q);
    } else {
      rateLimitHits++;
    }
    if (rateLimitHits >= 3 && results.size === 0) break;
  }
  return { results, rateLimited: rateLimitHits >= 3 && results.size === 0 };
}
var YAHOO_ONLY_SYMBOLS = /* @__PURE__ */ new Set([
  "^GSPC",
  "^DJI",
  "^IXIC",
  "^VIX",
  "GC=F",
  "CL=F",
  "NG=F",
  "SI=F",
  "HG=F"
]);
var CRYPTO_META = {
  bitcoin: { name: "Bitcoin", symbol: "BTC" },
  ethereum: { name: "Ethereum", symbol: "ETH" },
  solana: { name: "Solana", symbol: "SOL" },
  ripple: { name: "XRP", symbol: "XRP" }
};
async function fetchFinnhubQuote(symbol, apiKey) {
  try {
    const url = `https://finnhub.io/api/v1/quote?symbol=${encodeURIComponent(symbol)}&token=${apiKey}`;
    const resp = await fetch(url, {
      headers: { Accept: "application/json", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS3)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    if (data.c === 0 && data.h === 0 && data.l === 0) return null;
    return { symbol, price: data.c, changePercent: data.dp };
  } catch {
    return null;
  }
}
async function fetchYahooQuote(symbol) {
  try {
    await yahooGate();
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${encodeURIComponent(symbol)}`;
    const resp = await fetch(url, {
      headers: {
        "User-Agent": CHROME_UA
      },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS3)
    });
    if (!resp.ok) return null;
    const data = await resp.json();
    const result = data.chart.result[0];
    const meta = result?.meta;
    if (!meta) return null;
    const price = meta.regularMarketPrice;
    const prevClose = meta.chartPreviousClose || meta.previousClose || price;
    const change = (price - prevClose) / prevClose * 100;
    const closes = result.indicators?.quote?.[0]?.close;
    const sparkline = closes?.filter((v) => v != null) || [];
    return { price, change, sparkline };
  } catch {
    return null;
  }
}
async function fetchCoinGeckoMarkets(ids) {
  const url = `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=${ids.join(",")}&order=market_cap_desc&sparkline=true&price_change_percentage=24h`;
  const resp = await fetch(url, {
    headers: { Accept: "application/json", "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36" },
    signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS3)
  });
  if (!resp.ok) {
    const body = await resp.text().catch(() => "");
    throw new Error(`CoinGecko HTTP ${resp.status}: ${body.slice(0, 200)}`);
  }
  const data = await resp.json();
  if (!Array.isArray(data)) {
    throw new Error(`CoinGecko returned non-array: ${JSON.stringify(data).slice(0, 200)}`);
  }
  return data;
}

// server/worldmonitor/market/v1/list-market-quotes.ts
var REDIS_CACHE_KEY23 = "market:quotes:v1";
var REDIS_CACHE_TTL23 = 480;
var quotesCache = /* @__PURE__ */ new Map();
var QUOTES_CACHE_TTL = 48e4;
function cacheKey(symbols) {
  return [...symbols].sort().join(",");
}
function redisCacheKey(symbols) {
  return `${REDIS_CACHE_KEY23}:${[...symbols].sort().join(",")}`;
}
async function listMarketQuotes(_ctx, req) {
  const now = Date.now();
  const key = cacheKey(req.symbols);
  const memCached = quotesCache.get(key);
  if (memCached && now - memCached.timestamp < QUOTES_CACHE_TTL) {
    return memCached.data;
  }
  const redisKey = redisCacheKey(req.symbols);
  try {
    const result = await cachedFetchJson(redisKey, REDIS_CACHE_TTL23, async () => {
      const apiKey = process.env.FINNHUB_API_KEY;
      const symbols = req.symbols;
      if (!symbols.length) return { quotes: [], finnhubSkipped: !apiKey, skipReason: !apiKey ? "FINNHUB_API_KEY not configured" : "", rateLimited: false };
      const finnhubSymbols = symbols.filter((s) => !YAHOO_ONLY_SYMBOLS.has(s));
      const yahooSymbols = symbols.filter((s) => YAHOO_ONLY_SYMBOLS.has(s));
      const quotes = [];
      if (finnhubSymbols.length > 0 && apiKey) {
        const results = await Promise.all(
          finnhubSymbols.map((s) => fetchFinnhubQuote(s, apiKey))
        );
        for (const r of results) {
          if (r) {
            quotes.push({
              symbol: r.symbol,
              name: r.symbol,
              display: r.symbol,
              price: r.price,
              change: r.changePercent,
              sparkline: []
            });
          }
        }
      }
      const missedFinnhub = apiKey ? finnhubSymbols.filter((s) => !quotes.some((q) => q.symbol === s)) : finnhubSymbols;
      const allYahoo = [...yahooSymbols, ...missedFinnhub];
      let yahooRateLimited = false;
      if (allYahoo.length > 0) {
        const batch = await fetchYahooQuotesBatch(allYahoo);
        yahooRateLimited = batch.rateLimited;
        for (const s of allYahoo) {
          if (quotes.some((q) => q.symbol === s)) continue;
          const yahoo = batch.results.get(s);
          if (yahoo) {
            quotes.push({
              symbol: s,
              name: s,
              display: s,
              price: yahoo.price,
              change: yahoo.change,
              sparkline: yahoo.sparkline
            });
          }
        }
      }
      if (quotes.length === 0 && memCached) {
        return null;
      }
      if (quotes.length === 0) {
        return yahooRateLimited ? { quotes: [], finnhubSkipped: false, skipReason: "", rateLimited: true } : null;
      }
      const coveredByYahoo = finnhubSymbols.every((s) => quotes.some((q) => q.symbol === s));
      const skipped = !apiKey && !coveredByYahoo;
      return { quotes, finnhubSkipped: skipped, skipReason: skipped ? "FINNHUB_API_KEY not configured" : "", rateLimited: false };
    });
    if (result?.quotes?.length) {
      quotesCache.set(key, { data: result, timestamp: now });
    }
    return result || memCached?.data || { quotes: [], finnhubSkipped: false, skipReason: "", rateLimited: false };
  } catch {
    return memCached?.data || { quotes: [], finnhubSkipped: false, skipReason: "", rateLimited: false };
  }
}

// server/worldmonitor/market/v1/list-crypto-quotes.ts
var REDIS_CACHE_KEY24 = "market:crypto:v1";
var REDIS_CACHE_TTL24 = 600;
var fallbackCryptoCache = /* @__PURE__ */ new Map();
async function listCryptoQuotes(_ctx, req) {
  const ids = req.ids.length > 0 ? req.ids : Object.keys(CRYPTO_META);
  const cacheKey2 = `${REDIS_CACHE_KEY24}:${[...ids].sort().join(",")}`;
  try {
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL24, async () => {
      const items = await fetchCoinGeckoMarkets(ids);
      if (items.length === 0) {
        throw new Error("CoinGecko returned no data");
      }
      const byId = new Map(items.map((c) => [c.id, c]));
      const quotes = [];
      for (const id of ids) {
        const coin = byId.get(id);
        if (!coin) continue;
        const meta = CRYPTO_META[id];
        const prices = coin.sparkline_in_7d?.price;
        const sparkline = prices && prices.length > 24 ? prices.slice(-48) : prices || [];
        quotes.push({
          name: meta?.name || id,
          symbol: meta?.symbol || id.toUpperCase(),
          price: coin.current_price ?? 0,
          change: coin.price_change_percentage_24h ?? 0,
          sparkline
        });
      }
      if (quotes.every((q) => q.price === 0)) {
        throw new Error("CoinGecko returned all-zero prices");
      }
      return quotes.length > 0 ? { quotes } : null;
    });
    if (result) {
      if (fallbackCryptoCache.size > 50) fallbackCryptoCache.clear();
      fallbackCryptoCache.set(cacheKey2, { data: result, ts: Date.now() });
    }
    return result || fallbackCryptoCache.get(cacheKey2)?.data || { quotes: [] };
  } catch {
    return fallbackCryptoCache.get(cacheKey2)?.data || { quotes: [] };
  }
}

// server/worldmonitor/market/v1/list-commodity-quotes.ts
var REDIS_CACHE_KEY25 = "market:commodities:v1";
var REDIS_CACHE_TTL25 = 600;
var fallbackCommodityCache = /* @__PURE__ */ new Map();
function redisCacheKey2(symbols) {
  return `${REDIS_CACHE_KEY25}:${[...symbols].sort().join(",")}`;
}
async function listCommodityQuotes(_ctx, req) {
  const symbols = req.symbols;
  if (!symbols.length) return { quotes: [] };
  const redisKey = redisCacheKey2(symbols);
  try {
    const result = await cachedFetchJson(redisKey, REDIS_CACHE_TTL25, async () => {
      const batch = await fetchYahooQuotesBatch(symbols);
      const quotes = [];
      for (const s of symbols) {
        const yahoo = batch.results.get(s);
        if (yahoo) {
          quotes.push({ symbol: s, name: s, display: s, price: yahoo.price, change: yahoo.change, sparkline: yahoo.sparkline });
        }
      }
      return quotes.length > 0 ? { quotes } : null;
    });
    if (result) {
      if (fallbackCommodityCache.size > 50) fallbackCommodityCache.clear();
      fallbackCommodityCache.set(redisKey, { data: result, ts: Date.now() });
    }
    return result || fallbackCommodityCache.get(redisKey)?.data || { quotes: [] };
  } catch {
    return fallbackCommodityCache.get(redisKey)?.data || { quotes: [] };
  }
}

// server/worldmonitor/market/v1/get-sector-summary.ts
var REDIS_CACHE_KEY26 = "market:sectors:v1";
var REDIS_CACHE_TTL26 = 600;
var fallbackSectorCache = null;
async function getSectorSummary(_ctx, _req) {
  const apiKey = process.env.FINNHUB_API_KEY;
  try {
    const result = await cachedFetchJson(REDIS_CACHE_KEY26, REDIS_CACHE_TTL26, async () => {
      const sectorSymbols = ["XLK", "XLF", "XLE", "XLV", "XLY", "XLI", "XLP", "XLU", "XLB", "XLRE", "XLC", "SMH"];
      const sectors = [];
      if (apiKey) {
        const results = await Promise.all(
          sectorSymbols.map((s) => fetchFinnhubQuote(s, apiKey))
        );
        for (const r of results) {
          if (r) sectors.push({ symbol: r.symbol, name: r.symbol, change: r.changePercent });
        }
      }
      if (sectors.length === 0) {
        const batch = await fetchYahooQuotesBatch(sectorSymbols);
        for (const s of sectorSymbols) {
          const yahoo = batch.results.get(s);
          if (yahoo) sectors.push({ symbol: s, name: s, change: yahoo.change });
        }
      }
      return sectors.length > 0 ? { sectors } : null;
    });
    if (result) fallbackSectorCache = { data: result, ts: Date.now() };
    return result || fallbackSectorCache?.data || { sectors: [] };
  } catch {
    return fallbackSectorCache?.data || { sectors: [] };
  }
}

// server/worldmonitor/market/v1/list-stablecoin-markets.ts
var REDIS_CACHE_KEY27 = "market:stablecoins:v1";
var REDIS_CACHE_TTL27 = 600;
var DEFAULT_STABLECOIN_IDS = "tether,usd-coin,dai,first-digital-usd,ethena-usde";
var stablecoinCache = null;
var stablecoinCacheTimestamp = 0;
var STABLECOIN_CACHE_TTL = 48e4;
async function listStablecoinMarkets(_ctx, req) {
  const now = Date.now();
  if (stablecoinCache && now - stablecoinCacheTimestamp < STABLECOIN_CACHE_TTL) {
    return stablecoinCache;
  }
  const coins = req.coins.length > 0 ? req.coins.filter((c) => /^[a-z0-9-]+$/.test(c)).join(",") : DEFAULT_STABLECOIN_IDS;
  const redisKey = `${REDIS_CACHE_KEY27}:${coins}`;
  try {
    const result = await cachedFetchJson(redisKey, REDIS_CACHE_TTL27, async () => {
      const url = `https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids=${coins}&order=market_cap_desc&sparkline=false&price_change_percentage=7d`;
      const resp = await fetch(url, {
        headers: { Accept: "application/json", "User-Agent": CHROME_UA },
        signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS3)
      });
      if (resp.status === 429 && stablecoinCache) return null;
      if (!resp.ok) throw new Error(`CoinGecko HTTP ${resp.status}`);
      const data = await resp.json();
      const stablecoins = data.map((coin) => {
        const price = coin.current_price || 0;
        const deviation = Math.abs(price - 1);
        let pegStatus;
        if (deviation <= 5e-3) pegStatus = "ON PEG";
        else if (deviation <= 0.01) pegStatus = "SLIGHT DEPEG";
        else pegStatus = "DEPEGGED";
        return {
          id: coin.id,
          symbol: (coin.symbol || "").toUpperCase(),
          name: coin.name,
          price,
          deviation: +(deviation * 100).toFixed(3),
          pegStatus,
          marketCap: coin.market_cap || 0,
          volume24h: coin.total_volume || 0,
          change24h: coin.price_change_percentage_24h || 0,
          change7d: coin.price_change_percentage_7d_in_currency || 0,
          image: coin.image || ""
        };
      });
      if (stablecoins.length === 0) return null;
      const totalMarketCap = stablecoins.reduce((sum, c) => sum + c.marketCap, 0);
      const totalVolume24h = stablecoins.reduce((sum, c) => sum + c.volume24h, 0);
      const depeggedCount = stablecoins.filter((c) => c.pegStatus === "DEPEGGED").length;
      return {
        timestamp: (/* @__PURE__ */ new Date()).toISOString(),
        summary: {
          totalMarketCap,
          totalVolume24h,
          coinCount: stablecoins.length,
          depeggedCount,
          healthStatus: depeggedCount === 0 ? "HEALTHY" : depeggedCount === 1 ? "CAUTION" : "WARNING"
        },
        stablecoins
      };
    });
    if (result) {
      stablecoinCache = result;
      stablecoinCacheTimestamp = now;
    }
    return result || stablecoinCache || {
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      summary: {
        totalMarketCap: 0,
        totalVolume24h: 0,
        coinCount: 0,
        depeggedCount: 0,
        healthStatus: "UNAVAILABLE"
      },
      stablecoins: []
    };
  } catch {
    return stablecoinCache || {
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      summary: {
        totalMarketCap: 0,
        totalVolume24h: 0,
        coinCount: 0,
        depeggedCount: 0,
        healthStatus: "UNAVAILABLE"
      },
      stablecoins: []
    };
  }
}

// server/worldmonitor/market/v1/list-etf-flows.ts
var REDIS_CACHE_KEY28 = "market:etf-flows:v1";
var REDIS_CACHE_TTL28 = 600;
var ETF_LIST = [
  { ticker: "IBIT", issuer: "BlackRock" },
  { ticker: "FBTC", issuer: "Fidelity" },
  { ticker: "ARKB", issuer: "ARK/21Shares" },
  { ticker: "BITB", issuer: "Bitwise" },
  { ticker: "GBTC", issuer: "Grayscale" },
  { ticker: "HODL", issuer: "VanEck" },
  { ticker: "BRRR", issuer: "Valkyrie" },
  { ticker: "EZBC", issuer: "Franklin" },
  { ticker: "BTCO", issuer: "Invesco" },
  { ticker: "BTCW", issuer: "WisdomTree" }
];
var etfCache = null;
var etfCacheTimestamp = 0;
var ETF_CACHE_TTL = 9e5;
async function fetchEtfChart(ticker) {
  try {
    await yahooGate();
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${ticker}?range=5d&interval=1d`;
    const resp = await fetch(url, {
      headers: {
        "User-Agent": CHROME_UA
      },
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS3)
    });
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}
function parseEtfChartData(chart, ticker, issuer) {
  try {
    const result = chart?.chart?.result?.[0];
    if (!result) return null;
    const quote = result.indicators?.quote?.[0];
    const closes = quote?.close || [];
    const volumes = quote?.volume || [];
    const validCloses = closes.filter((p) => p != null);
    const validVolumes = volumes.filter((v) => v != null);
    if (validCloses.length < 2) return null;
    const latestPrice = validCloses[validCloses.length - 1];
    const prevPrice = validCloses[validCloses.length - 2];
    const priceChange = prevPrice ? (latestPrice - prevPrice) / prevPrice * 100 : 0;
    const latestVolume = validVolumes.length > 0 ? validVolumes[validVolumes.length - 1] : 0;
    const avgVolume = validVolumes.length > 1 ? validVolumes.slice(0, -1).reduce((a, b) => a + b, 0) / (validVolumes.length - 1) : latestVolume;
    const volumeRatio = avgVolume > 0 ? latestVolume / avgVolume : 1;
    const direction = priceChange > 0.1 ? "inflow" : priceChange < -0.1 ? "outflow" : "neutral";
    const estFlowMagnitude = latestVolume * latestPrice * (priceChange > 0 ? 1 : -1) * 0.1;
    return {
      ticker,
      issuer,
      price: +latestPrice.toFixed(2),
      priceChange: +priceChange.toFixed(2),
      volume: latestVolume,
      avgVolume: Math.round(avgVolume),
      volumeRatio: +volumeRatio.toFixed(2),
      direction,
      estFlow: Math.round(estFlowMagnitude)
    };
  } catch {
    return null;
  }
}
async function listEtfFlows(_ctx, _req) {
  const now = Date.now();
  if (etfCache && now - etfCacheTimestamp < ETF_CACHE_TTL) {
    return etfCache;
  }
  try {
    const result = await cachedFetchJson(REDIS_CACHE_KEY28, REDIS_CACHE_TTL28, async () => {
      const etfs = [];
      let misses = 0;
      for (const etf of ETF_LIST) {
        const chart = await fetchEtfChart(etf.ticker);
        if (chart) {
          const parsed = parseEtfChartData(chart, etf.ticker, etf.issuer);
          if (parsed) etfs.push(parsed);
          else misses++;
        } else {
          misses++;
        }
        if (misses >= 3 && etfs.length === 0) break;
      }
      if (etfs.length === 0 && etfCache) {
        return null;
      }
      if (etfs.length === 0) {
        return misses >= 3 ? { timestamp: (/* @__PURE__ */ new Date()).toISOString(), etfs: [], rateLimited: true } : null;
      }
      const totalVolume = etfs.reduce((sum, e) => sum + e.volume, 0);
      const totalEstFlow = etfs.reduce((sum, e) => sum + e.estFlow, 0);
      const inflowCount = etfs.filter((e) => e.direction === "inflow").length;
      const outflowCount = etfs.filter((e) => e.direction === "outflow").length;
      etfs.sort((a, b) => b.volume - a.volume);
      return {
        timestamp: (/* @__PURE__ */ new Date()).toISOString(),
        summary: {
          etfCount: etfs.length,
          totalVolume,
          totalEstFlow,
          netDirection: totalEstFlow > 0 ? "NET INFLOW" : totalEstFlow < 0 ? "NET OUTFLOW" : "NEUTRAL",
          inflowCount,
          outflowCount
        },
        etfs,
        rateLimited: false
      };
    });
    if (result) {
      etfCache = result;
      etfCacheTimestamp = now;
    }
    return result || etfCache || {
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      summary: {
        etfCount: 0,
        totalVolume: 0,
        totalEstFlow: 0,
        netDirection: "UNAVAILABLE",
        inflowCount: 0,
        outflowCount: 0
      },
      etfs: [],
      rateLimited: false
    };
  } catch {
    return etfCache || {
      timestamp: (/* @__PURE__ */ new Date()).toISOString(),
      summary: {
        etfCount: 0,
        totalVolume: 0,
        totalEstFlow: 0,
        netDirection: "UNAVAILABLE",
        inflowCount: 0,
        outflowCount: 0
      },
      etfs: [],
      rateLimited: false
    };
  }
}

// server/worldmonitor/market/v1/get-country-stock-index.ts
var COUNTRY_INDEX = {
  US: { symbol: "^GSPC", name: "S&P 500" },
  GB: { symbol: "^FTSE", name: "FTSE 100" },
  DE: { symbol: "^GDAXI", name: "DAX" },
  FR: { symbol: "^FCHI", name: "CAC 40" },
  JP: { symbol: "^N225", name: "Nikkei 225" },
  CN: { symbol: "000001.SS", name: "SSE Composite" },
  HK: { symbol: "^HSI", name: "Hang Seng" },
  IN: { symbol: "^BSESN", name: "BSE Sensex" },
  KR: { symbol: "^KS11", name: "KOSPI" },
  TW: { symbol: "^TWII", name: "TAIEX" },
  AU: { symbol: "^AXJO", name: "ASX 200" },
  BR: { symbol: "^BVSP", name: "Bovespa" },
  CA: { symbol: "^GSPTSE", name: "TSX Composite" },
  MX: { symbol: "^MXX", name: "IPC Mexico" },
  AR: { symbol: "^MERV", name: "MERVAL" },
  RU: { symbol: "IMOEX.ME", name: "MOEX" },
  ZA: { symbol: "^J203.JO", name: "JSE All Share" },
  SA: { symbol: "^TASI.SR", name: "Tadawul" },
  AE: { symbol: "DFMGI.AE", name: "DFM General" },
  IL: { symbol: "^TA125.TA", name: "TA-125" },
  TR: { symbol: "XU100.IS", name: "BIST 100" },
  PL: { symbol: "^WIG20", name: "WIG 20" },
  NL: { symbol: "^AEX", name: "AEX" },
  CH: { symbol: "^SSMI", name: "SMI" },
  ES: { symbol: "^IBEX", name: "IBEX 35" },
  IT: { symbol: "FTSEMIB.MI", name: "FTSE MIB" },
  SE: { symbol: "^OMX", name: "OMX Stockholm 30" },
  NO: { symbol: "^OSEAX", name: "Oslo All Share" },
  SG: { symbol: "^STI", name: "STI" },
  TH: { symbol: "^SET.BK", name: "SET" },
  MY: { symbol: "^KLSE", name: "KLCI" },
  ID: { symbol: "^JKSE", name: "Jakarta Composite" },
  PH: { symbol: "PSEI.PS", name: "PSEi" },
  NZ: { symbol: "^NZ50", name: "NZX 50" },
  EG: { symbol: "^EGX30.CA", name: "EGX 30" },
  CL: { symbol: "^IPSA", name: "IPSA" },
  PE: { symbol: "^SPBLPGPT", name: "S&P Lima" },
  AT: { symbol: "^ATX", name: "ATX" },
  BE: { symbol: "^BFX", name: "BEL 20" },
  FI: { symbol: "^OMXH25", name: "OMX Helsinki 25" },
  DK: { symbol: "^OMXC25", name: "OMX Copenhagen 25" },
  IE: { symbol: "^ISEQ", name: "ISEQ Overall" },
  PT: { symbol: "^PSI20", name: "PSI 20" },
  CZ: { symbol: "^PX", name: "PX Prague" },
  HU: { symbol: "^BUX", name: "BUX" }
};
var REDIS_CACHE_KEY29 = "market:stock-index:v1";
var REDIS_CACHE_TTL29 = 1800;
var stockIndexCache = {};
var STOCK_INDEX_CACHE_TTL = 36e5;
async function getCountryStockIndex(_ctx, req) {
  const code = (req.countryCode || "").toUpperCase();
  const notAvailable = {
    available: false,
    code,
    symbol: "",
    indexName: "",
    price: 0,
    weekChangePercent: 0,
    currency: "",
    fetchedAt: ""
  };
  if (!code) return notAvailable;
  const index = COUNTRY_INDEX[code];
  if (!index) return notAvailable;
  const cached = stockIndexCache[code];
  if (cached && Date.now() - cached.ts < STOCK_INDEX_CACHE_TTL) return cached.data;
  const redisKey = `${REDIS_CACHE_KEY29}:${code}`;
  try {
    const result = await cachedFetchJson(redisKey, REDIS_CACHE_TTL29, async () => {
      const encodedSymbol = encodeURIComponent(index.symbol);
      const yahooUrl = `https://query1.finance.yahoo.com/v8/finance/chart/${encodedSymbol}?range=1mo&interval=1d`;
      const res = await fetch(yahooUrl, {
        headers: { "User-Agent": CHROME_UA },
        signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS3)
      });
      if (!res.ok) return null;
      const data = await res.json();
      const chartResult = data?.chart?.result?.[0];
      if (!chartResult) return null;
      const allCloses = chartResult.indicators?.quote?.[0]?.close?.filter((v) => v != null);
      if (!allCloses || allCloses.length < 2) return null;
      const closes = allCloses.slice(-6);
      const latest = closes[closes.length - 1];
      const oldest = closes[0];
      const weekChange = (latest - oldest) / oldest * 100;
      const meta = chartResult.meta || {};
      return {
        available: true,
        code,
        symbol: index.symbol,
        indexName: index.name,
        price: +latest.toFixed(2),
        weekChangePercent: +weekChange.toFixed(2),
        currency: meta.currency || "USD",
        fetchedAt: (/* @__PURE__ */ new Date()).toISOString()
      };
    });
    if (result?.available) {
      stockIndexCache[code] = { data: result, ts: Date.now() };
    }
    return result || stockIndexCache[code]?.data || notAvailable;
  } catch {
    return stockIndexCache[code]?.data || notAvailable;
  }
}

// server/worldmonitor/market/v1/list-gulf-quotes.ts
var REDIS_KEY2 = "market:gulf-quotes:v1";
var REDIS_TTL = 480;
var memCache = null;
var MEM_TTL = 48e4;
var GULF_SYMBOLS = [
  // Indices — real Yahoo indices where available, iShares ETF proxies otherwise
  { symbol: "^TASI.SR", name: "Tadawul All Share", country: "Saudi Arabia", flag: "\u{1F1F8}\u{1F1E6}", type: "index" },
  { symbol: "DFMGI.AE", name: "Dubai Financial Market", country: "UAE", flag: "\u{1F1E6}\u{1F1EA}", type: "index" },
  { symbol: "UAE", name: "Abu Dhabi (iShares)", country: "UAE", flag: "\u{1F1E6}\u{1F1EA}", type: "index" },
  { symbol: "QAT", name: "Qatar (iShares)", country: "Qatar", flag: "\u{1F1F6}\u{1F1E6}", type: "index" },
  { symbol: "GULF", name: "Gulf Dividend (WisdomTree)", country: "Kuwait", flag: "\u{1F1F0}\u{1F1FC}", type: "index" },
  { symbol: "^MSM", name: "Muscat MSM 30", country: "Oman", flag: "\u{1F1F4}\u{1F1F2}", type: "index" },
  // Currencies (6)
  { symbol: "SARUSD=X", name: "Saudi Riyal", country: "Saudi Arabia", flag: "\u{1F1F8}\u{1F1E6}", type: "currency" },
  { symbol: "AEDUSD=X", name: "UAE Dirham", country: "UAE", flag: "\u{1F1E6}\u{1F1EA}", type: "currency" },
  { symbol: "QARUSD=X", name: "Qatari Riyal", country: "Qatar", flag: "\u{1F1F6}\u{1F1E6}", type: "currency" },
  { symbol: "KWDUSD=X", name: "Kuwaiti Dinar", country: "Kuwait", flag: "\u{1F1F0}\u{1F1FC}", type: "currency" },
  { symbol: "BHDUSD=X", name: "Bahraini Dinar", country: "Bahrain", flag: "\u{1F1E7}\u{1F1ED}", type: "currency" },
  { symbol: "OMRUSD=X", name: "Omani Rial", country: "Oman", flag: "\u{1F1F4}\u{1F1F2}", type: "currency" },
  // Oil benchmarks (2)
  { symbol: "CL=F", name: "WTI Crude", country: "", flag: "\u{1F6E2}\uFE0F", type: "oil" },
  { symbol: "BZ=F", name: "Brent Crude", country: "", flag: "\u{1F6E2}\uFE0F", type: "oil" }
];
var ALL_SYMBOLS = GULF_SYMBOLS.map((s) => s.symbol);
var META_MAP = new Map(GULF_SYMBOLS.map((s) => [s.symbol, s]));
async function listGulfQuotes(_ctx, _req) {
  const now = Date.now();
  if (memCache && now - memCache.ts < MEM_TTL) {
    return memCache.data;
  }
  try {
    const result = await cachedFetchJson(REDIS_KEY2, REDIS_TTL, async () => {
      const batch = await fetchYahooQuotesBatch(ALL_SYMBOLS);
      const quotes = [];
      for (const sym of ALL_SYMBOLS) {
        const yahoo = batch.results.get(sym);
        const meta = META_MAP.get(sym);
        if (yahoo) {
          quotes.push({
            symbol: sym,
            name: meta.name,
            country: meta.country,
            flag: meta.flag,
            type: meta.type,
            price: yahoo.price,
            change: yahoo.change,
            sparkline: yahoo.sparkline
          });
        }
      }
      if (quotes.length === 0 && memCache) return null;
      if (quotes.length === 0) {
        return batch.rateLimited ? { quotes: [], rateLimited: true } : null;
      }
      return { quotes, rateLimited: false };
    });
    if (result?.quotes?.length) {
      memCache = { data: result, ts: now };
    }
    return result || memCache?.data || { quotes: [], rateLimited: false };
  } catch {
    return memCache?.data || { quotes: [], rateLimited: false };
  }
}

// server/worldmonitor/market/v1/handler.ts
var marketHandler = {
  listMarketQuotes,
  listCryptoQuotes,
  listCommodityQuotes,
  getSectorSummary,
  listStablecoinMarkets,
  listEtfFlows,
  getCountryStockIndex,
  listGulfQuotes
};

// src/generated/server/worldmonitor/news/v1/service_server.ts
var ValidationError15 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createNewsServiceRoutes(handler2, options) {
  return [
    {
      method: "POST",
      path: "/api/news/v1/summarize-article",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = await req.json();
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("summarizeArticle", body);
            if (bodyViolations) {
              throw new ValidationError15(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.summarizeArticle(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError15) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/news/v1/list-feed-digest",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            variant: params.get("variant") ?? "",
            lang: params.get("lang") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listFeedDigest", body);
            if (bodyViolations) {
              throw new ValidationError15(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listFeedDigest(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError15) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/_shared/hash.ts
function hashString(input) {
  let h = 0xcbf29ce484222325n;
  const FNV_PRIME = 0x100000001b3n;
  const MASK_52 = (1n << 52n) - 1n;
  for (let i = 0; i < input.length; i++) {
    h ^= BigInt(input.charCodeAt(i));
    h = h * FNV_PRIME & MASK_52;
  }
  return Number(h).toString(36);
}

// server/worldmonitor/news/v1/dedup.mjs
function deduplicateHeadlines(headlines) {
  const seen = [];
  const unique = [];
  for (const headline of headlines) {
    const normalized = headline.toLowerCase().replace(/[^\w\s]/g, "").replace(/\s+/g, " ").trim();
    const words = new Set(normalized.split(" ").filter((w) => w.length >= 4));
    let isDuplicate = false;
    for (const seenWords of seen) {
      const intersection = [...words].filter((w) => seenWords.has(w));
      const similarity = intersection.length / Math.min(words.size, seenWords.size);
      if (similarity > 0.6) {
        isDuplicate = true;
        break;
      }
    }
    if (!isDuplicate) {
      seen.push(words);
      unique.push(headline);
    }
  }
  return unique;
}

// server/worldmonitor/news/v1/_shared.ts
var CACHE_TTL_SECONDS = 86400;
var CACHE_VERSION = "v5";
function getCacheKey(headlines, mode, geoContext = "", variant = "full", lang = "en") {
  const sorted = headlines.slice(0, 5).sort().join("|");
  const geoHash = geoContext ? ":g" + hashString(geoContext).slice(0, 6) : "";
  const hash = hashString(`${mode}:${sorted}`);
  const normalizedVariant = typeof variant === "string" && variant ? variant.toLowerCase() : "full";
  const normalizedLang = typeof lang === "string" && lang ? lang.toLowerCase() : "en";
  if (mode === "translate") {
    const targetLang = normalizedVariant || normalizedLang;
    return `summary:${CACHE_VERSION}:${mode}:${targetLang}:${hash}${geoHash}`;
  }
  return `summary:${CACHE_VERSION}:${mode}:${normalizedVariant}:${normalizedLang}:${hash}${geoHash}`;
}
function buildArticlePrompts(headlines, uniqueHeadlines, opts) {
  const headlineText = uniqueHeadlines.map((h, i) => `${i + 1}. ${h}`).join("\n");
  const intelSection = opts.geoContext ? `

${opts.geoContext}` : "";
  const isTechVariant = opts.variant === "tech";
  const dateContext = `Current date: ${(/* @__PURE__ */ new Date()).toISOString().split("T")[0]}.${isTechVariant ? "" : " Provide geopolitical context appropriate for the current date."}`;
  const langInstruction = opts.lang && opts.lang !== "en" ? `
IMPORTANT: Output the summary in ${opts.lang.toUpperCase()} language.` : "";
  let systemPrompt;
  let userPrompt;
  if (opts.mode === "brief") {
    if (isTechVariant) {
      systemPrompt = `${dateContext}

Summarize the single most important tech/startup headline in 2 concise sentences MAX (under 60 words total).
Rules:
- Each numbered headline below is a SEPARATE, UNRELATED story
- Pick the ONE most significant headline and summarize ONLY that story
- NEVER combine or merge facts, names, or details from different headlines
- Focus ONLY on technology, startups, AI, funding, product launches, or developer news
- IGNORE political news, trade policy, tariffs, government actions unless directly about tech regulation
- Lead with the company/product/technology name
- No bullet points, no meta-commentary, no elaboration beyond the core facts${langInstruction}`;
    } else {
      systemPrompt = `${dateContext}

Summarize the single most important headline in 2 concise sentences MAX (under 60 words total).
Rules:
- Each numbered headline below is a SEPARATE, UNRELATED story
- Pick the ONE most significant headline and summarize ONLY that story
- NEVER combine or merge people, places, or facts from different headlines into one sentence
- Lead with WHAT happened and WHERE - be specific
- NEVER start with "Breaking news", "Good evening", "Tonight", or TV-style openings
- Start directly with the subject of the chosen headline
- If intelligence context is provided, use it only if it relates to your chosen headline
- No bullet points, no meta-commentary, no elaboration beyond the core facts${langInstruction}`;
    }
    userPrompt = `Each headline below is a separate story. Pick the most important ONE and summarize only that story:
${headlineText}${intelSection}`;
  } else if (opts.mode === "analysis") {
    if (isTechVariant) {
      systemPrompt = `${dateContext}

Analyze the most significant tech/startup development in 2 concise sentences MAX (under 60 words total).
Rules:
- Each numbered headline below is a SEPARATE, UNRELATED story
- Pick the ONE most significant story and analyze ONLY that
- NEVER combine facts from different headlines
- Focus ONLY on technology implications: funding trends, AI developments, market shifts, product strategy
- IGNORE political implications, trade wars, government unless directly about tech policy
- Lead with the insight, no filler or elaboration`;
    } else {
      systemPrompt = `${dateContext}

Analyze the most significant development in 2 concise sentences MAX (under 60 words total). Be direct and specific.
Rules:
- Each numbered headline below is a SEPARATE, UNRELATED story
- Pick the ONE most significant story and analyze ONLY that
- NEVER combine or merge people, places, or facts from different headlines
- Lead with the insight - what's significant and why
- NEVER start with "Breaking news", "Tonight", "The key/dominant narrative is"
- Start with substance, no filler or elaboration
- If intelligence context is provided, use it only if it relates to your chosen headline`;
    }
    userPrompt = isTechVariant ? `Each headline is a separate story. What's the key tech trend?
${headlineText}${intelSection}` : `Each headline is a separate story. What's the key pattern or risk?
${headlineText}${intelSection}`;
  } else if (opts.mode === "translate") {
    const targetLang = opts.variant;
    systemPrompt = `You are a professional news translator. Translate the following news headlines/summaries into ${targetLang}.
Rules:
- Maintain the original tone and journalistic style.
- Do NOT add any conversational filler (e.g., "Here is the translation").
- Output ONLY the translated text.
- If the text is already in ${targetLang}, return it as is.`;
    userPrompt = `Translate to ${targetLang}:
${headlines[0]}`;
  } else {
    systemPrompt = isTechVariant ? `${dateContext}

Pick the most important tech headline and summarize it in 2 concise sentences (under 60 words). Each headline is a separate story - NEVER merge facts from different headlines. Focus on startups, AI, funding, products. Ignore politics unless directly about tech regulation.${langInstruction}` : `${dateContext}

Pick the most important headline and summarize it in 2 concise sentences (under 60 words). Each headline is a separate, unrelated story - NEVER merge people or facts from different headlines. Lead with substance. NEVER start with "Breaking news" or "Tonight".${langInstruction}`;
    userPrompt = `Each headline is a separate story. Key takeaway from the most important one:
${headlineText}${intelSection}`;
  }
  return { systemPrompt, userPrompt };
}
function getProviderCredentials(provider) {
  if (provider === "ollama") {
    const baseUrl = process.env.OLLAMA_API_URL;
    if (!baseUrl) return null;
    const headers = { "Content-Type": "application/json" };
    const apiKey = process.env.OLLAMA_API_KEY;
    if (apiKey) {
      headers["Authorization"] = `Bearer ${apiKey}`;
    }
    const rawMax = parseInt(process.env.OLLAMA_MAX_TOKENS || "300", 10);
    const ollamaMaxTokens = Number.isFinite(rawMax) ? Math.min(Math.max(rawMax, 50), 2e3) : 300;
    return {
      apiUrl: new URL("/v1/chat/completions", baseUrl).toString(),
      model: process.env.OLLAMA_MODEL || "llama3.1:8b",
      headers,
      extraBody: { think: false, max_tokens: ollamaMaxTokens }
    };
  }
  if (provider === "groq") {
    const apiKey = process.env.GROQ_API_KEY;
    if (!apiKey) return null;
    return {
      apiUrl: "https://api.groq.com/openai/v1/chat/completions",
      model: "llama-3.1-8b-instant",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json"
      }
    };
  }
  if (provider === "openrouter") {
    const apiKey = process.env.OPENROUTER_API_KEY;
    if (!apiKey) return null;
    return {
      apiUrl: "https://openrouter.ai/api/v1/chat/completions",
      model: "openrouter/free",
      headers: {
        "Authorization": `Bearer ${apiKey}`,
        "Content-Type": "application/json",
        "HTTP-Referer": "https://worldmonitor.app",
        "X-Title": "WorldMonitor"
      }
    };
  }
  return null;
}

// server/worldmonitor/news/v1/summarize-article.ts
var TASK_NARRATION = /^(we need to|i need to|let me|i'll |i should|i will |the task is|the instructions|according to the rules|so we need to|okay[,.]\s*(i'll|let me|so|we need|the task|i should|i will)|sure[,.]\s*(i'll|let me|so|we need|the task|i should|i will|here)|first[, ]+(i|we|let)|to summarize (the headlines|the task|this)|my task (is|was|:)|step \d)/i;
var PROMPT_ECHO = /^(summarize the top story|summarize the key|rules:|here are the rules|the top story is likely)/i;
function hasReasoningPreamble(text) {
  const trimmed = text.trim();
  return TASK_NARRATION.test(trimmed) || PROMPT_ECHO.test(trimmed);
}
async function summarizeArticle(_ctx, req) {
  const { provider, mode = "brief", geoContext = "", variant = "full", lang = "en" } = req;
  const MAX_HEADLINES = 10;
  const MAX_HEADLINE_LEN = 500;
  const MAX_GEO_CONTEXT_LEN = 2e3;
  const headlines = (req.headlines || []).slice(0, MAX_HEADLINES).map((h) => typeof h === "string" ? h.slice(0, MAX_HEADLINE_LEN) : "");
  const sanitizedGeoContext = typeof geoContext === "string" ? geoContext.slice(0, MAX_GEO_CONTEXT_LEN) : "";
  const skipReasons = {
    ollama: "OLLAMA_API_URL not configured",
    groq: "GROQ_API_KEY not configured",
    openrouter: "OPENROUTER_API_KEY not configured"
  };
  const credentials = getProviderCredentials(provider);
  if (!credentials) {
    return {
      summary: "",
      model: "",
      provider,
      cached: false,
      tokens: 0,
      fallback: true,
      skipped: true,
      reason: skipReasons[provider] || `Unknown provider: ${provider}`,
      error: "",
      errorType: ""
    };
  }
  const { apiUrl, model, headers: providerHeaders, extraBody } = credentials;
  if (!headlines || !Array.isArray(headlines) || headlines.length === 0) {
    return {
      summary: "",
      model: "",
      provider,
      cached: false,
      tokens: 0,
      fallback: false,
      skipped: false,
      reason: "",
      error: "Headlines array required",
      errorType: "ValidationError"
    };
  }
  try {
    const cacheKey2 = getCacheKey(headlines, mode, sanitizedGeoContext, variant, lang);
    const { data: result, source } = await cachedFetchJsonWithMeta(
      cacheKey2,
      CACHE_TTL_SECONDS,
      async () => {
        const uniqueHeadlines = deduplicateHeadlines(headlines.slice(0, 5));
        const { systemPrompt, userPrompt } = buildArticlePrompts(headlines, uniqueHeadlines, {
          mode,
          geoContext: sanitizedGeoContext,
          variant,
          lang
        });
        const response = await fetch(apiUrl, {
          method: "POST",
          headers: { ...providerHeaders, "User-Agent": CHROME_UA },
          body: JSON.stringify({
            model,
            messages: [
              { role: "system", content: systemPrompt },
              { role: "user", content: userPrompt }
            ],
            temperature: 0.3,
            max_tokens: 100,
            top_p: 0.9,
            ...extraBody
          }),
          signal: AbortSignal.timeout(3e4)
        });
        if (!response.ok) {
          const errorText = await response.text();
          console.error(`[SummarizeArticle:${provider}] API error:`, response.status, errorText);
          throw new Error(response.status === 429 ? "Rate limited" : `${provider} API error`);
        }
        const data = await response.json();
        const tokens = data.usage?.total_tokens || 0;
        const message = data.choices?.[0]?.message;
        let rawContent = typeof message?.content === "string" ? message.content.trim() : "";
        rawContent = rawContent.replace(/<think>[\s\S]*?<\/think>/gi, "").replace(/<\|thinking\|>[\s\S]*?<\|\/thinking\|>/gi, "").replace(/<reasoning>[\s\S]*?<\/reasoning>/gi, "").replace(/<reflection>[\s\S]*?<\/reflection>/gi, "").replace(/<\|begin_of_thought\|>[\s\S]*?<\|end_of_thought\|>/gi, "").trim();
        rawContent = rawContent.replace(/<think>[\s\S]*/gi, "").replace(/<\|thinking\|>[\s\S]*/gi, "").replace(/<reasoning>[\s\S]*/gi, "").replace(/<reflection>[\s\S]*/gi, "").replace(/<\|begin_of_thought\|>[\s\S]*/gi, "").trim();
        if (["brief", "analysis"].includes(mode) && rawContent.length < 20) {
          console.warn(`[SummarizeArticle:${provider}] Output too short after stripping (${rawContent.length} chars), rejecting`);
          return null;
        }
        if (["brief", "analysis"].includes(mode) && hasReasoningPreamble(rawContent)) {
          console.warn(`[SummarizeArticle:${provider}] Reasoning preamble detected, rejecting`);
          return null;
        }
        return rawContent ? { summary: rawContent, model, tokens } : null;
      }
    );
    if (result?.summary) {
      return {
        summary: result.summary,
        model: result.model || model,
        provider: source === "cache" ? "cache" : provider,
        cached: source === "cache",
        tokens: source === "cache" ? 0 : result.tokens || 0,
        fallback: false,
        skipped: false,
        reason: "",
        error: "",
        errorType: ""
      };
    }
    return {
      summary: "",
      model: "",
      provider,
      cached: false,
      tokens: 0,
      fallback: true,
      skipped: false,
      reason: "",
      error: "Empty response",
      errorType: ""
    };
  } catch (err) {
    const error = err instanceof Error ? err : new Error(String(err));
    console.error(`[SummarizeArticle:${provider}] Error:`, error.name, error.message);
    return {
      summary: "",
      model: "",
      provider,
      cached: false,
      tokens: 0,
      fallback: true,
      skipped: false,
      reason: "",
      error: error.message,
      errorType: error.name
    };
  }
}

// server/worldmonitor/news/v1/_feeds.ts
var gn = (q) => `https://news.google.com/rss/search?q=${encodeURIComponent(q)}&hl=en-US&gl=US&ceid=US:en`;
var VARIANT_FEEDS = {
  full: {
    politics: [
      { name: "BBC World", url: "https://feeds.bbci.co.uk/news/world/rss.xml" },
      { name: "Guardian World", url: "https://www.theguardian.com/world/rss" },
      { name: "AP News", url: gn("site:apnews.com") },
      { name: "Reuters World", url: gn("site:reuters.com world") },
      { name: "CNN World", url: gn("site:cnn.com world news when:1d") }
    ],
    us: [
      { name: "NPR News", url: "https://feeds.npr.org/1001/rss.xml" },
      { name: "Politico", url: gn("site:politico.com when:1d") },
      { name: "Axios", url: "https://api.axios.com/feed/" }
    ],
    europe: [
      { name: "France 24", url: "https://www.france24.com/en/rss" },
      { name: "EuroNews", url: "https://www.euronews.com/rss?format=xml" },
      { name: "Le Monde", url: "https://www.lemonde.fr/en/rss/une.xml" },
      { name: "DW News", url: "https://rss.dw.com/xml/rss-en-all" }
    ],
    middleeast: [
      { name: "BBC Middle East", url: "https://feeds.bbci.co.uk/news/world/middle_east/rss.xml" },
      { name: "Al Jazeera", url: "https://www.aljazeera.com/xml/rss/all.xml" },
      { name: "Guardian ME", url: "https://www.theguardian.com/world/middleeast/rss" },
      { name: "Oman Observer", url: "https://www.omanobserver.om/rssFeed/1" }
    ],
    tech: [
      { name: "Hacker News", url: "https://hnrss.org/frontpage" },
      { name: "Ars Technica", url: "https://feeds.arstechnica.com/arstechnica/technology-lab" },
      { name: "The Verge", url: "https://www.theverge.com/rss/index.xml" },
      { name: "MIT Tech Review", url: "https://www.technologyreview.com/feed/" }
    ],
    ai: [
      { name: "AI News", url: gn('(OpenAI OR Anthropic OR Google AI OR "large language model" OR ChatGPT) when:2d') },
      { name: "VentureBeat AI", url: "https://venturebeat.com/category/ai/feed/" },
      { name: "The Verge AI", url: "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml" },
      { name: "MIT Tech Review", url: "https://www.technologyreview.com/topic/artificial-intelligence/feed" },
      { name: "ArXiv AI", url: "https://export.arxiv.org/rss/cs.AI" }
    ],
    finance: [
      { name: "CNBC", url: "https://www.cnbc.com/id/100003114/device/rss/rss.html" },
      { name: "MarketWatch", url: gn("site:marketwatch.com markets when:1d") },
      { name: "Yahoo Finance", url: "https://finance.yahoo.com/news/rssindex" },
      { name: "Financial Times", url: "https://www.ft.com/rss/home" },
      { name: "Reuters Business", url: gn("site:reuters.com business markets") }
    ],
    gov: [
      { name: "White House", url: gn("site:whitehouse.gov") },
      { name: "State Dept", url: gn('site:state.gov OR "State Department"') },
      { name: "Pentagon", url: gn("site:defense.gov OR Pentagon") },
      { name: "Federal Reserve", url: "https://www.federalreserve.gov/feeds/press_all.xml" },
      { name: "SEC", url: "https://www.sec.gov/news/pressreleases.rss" },
      { name: "UN News", url: "https://news.un.org/feed/subscribe/en/news/all/rss.xml" },
      { name: "CISA", url: "https://www.cisa.gov/cybersecurity-advisories/all.xml" }
    ],
    africa: [
      { name: "BBC Africa", url: "https://feeds.bbci.co.uk/news/world/africa/rss.xml" },
      { name: "News24", url: "https://feeds.news24.com/articles/news24/TopStories/rss" }
    ],
    latam: [
      { name: "BBC Latin America", url: "https://feeds.bbci.co.uk/news/world/latin_america/rss.xml" },
      { name: "Guardian Americas", url: "https://www.theguardian.com/world/americas/rss" }
    ],
    asia: [
      { name: "BBC Asia", url: "https://feeds.bbci.co.uk/news/world/asia/rss.xml" },
      { name: "The Diplomat", url: "https://thediplomat.com/feed/" },
      { name: "Nikkei Asia", url: gn("site:asia.nikkei.com when:3d") },
      { name: "CNA", url: "https://www.channelnewsasia.com/api/v1/rss-outbound-feed?_format=xml" },
      { name: "NDTV", url: "https://feeds.feedburner.com/ndtvnews-top-stories" }
    ],
    energy: [
      { name: "Oil & Gas", url: gn('(oil price OR OPEC OR "natural gas" OR pipeline OR LNG) when:2d') }
    ],
    thinktanks: [
      { name: "Foreign Policy", url: "https://foreignpolicy.com/feed/" },
      { name: "Atlantic Council", url: "https://www.atlanticcouncil.org/feed/" },
      { name: "Foreign Affairs", url: "https://www.foreignaffairs.com/rss.xml" }
    ],
    crisis: [
      { name: "CrisisWatch", url: "https://www.crisisgroup.org/rss" },
      { name: "IAEA", url: "https://www.iaea.org/feeds/topnews" },
      { name: "WHO", url: "https://www.who.int/rss-feeds/news-english.xml" }
    ],
    layoffs: [
      { name: "TechCrunch Layoffs", url: "https://techcrunch.com/tag/layoffs/feed/" }
    ]
  },
  tech: {
    tech: [
      { name: "TechCrunch", url: "https://techcrunch.com/feed/" },
      { name: "The Verge", url: "https://www.theverge.com/rss/index.xml" },
      { name: "Ars Technica", url: "https://feeds.arstechnica.com/arstechnica/technology-lab" },
      { name: "Hacker News", url: "https://hnrss.org/frontpage" }
    ],
    ai: [
      { name: "AI News", url: gn('(OpenAI OR Anthropic OR Google AI OR "large language model" OR ChatGPT) when:2d') },
      { name: "VentureBeat AI", url: "https://venturebeat.com/category/ai/feed/" },
      { name: "The Verge AI", url: "https://www.theverge.com/rss/ai-artificial-intelligence/index.xml" },
      { name: "ArXiv AI", url: "https://export.arxiv.org/rss/cs.AI" }
    ],
    startups: [
      { name: "TechCrunch Startups", url: "https://techcrunch.com/category/startups/feed/" },
      { name: "VentureBeat", url: "https://venturebeat.com/feed/" },
      { name: "Crunchbase News", url: "https://news.crunchbase.com/feed/" }
    ],
    security: [
      { name: "Krebs Security", url: "https://krebsonsecurity.com/feed/" },
      { name: "Dark Reading", url: "https://www.darkreading.com/rss.xml" }
    ],
    github: [
      { name: "GitHub Blog", url: "https://github.blog/feed/" }
    ],
    funding: [
      { name: "VC News", url: gn('("Series A" OR "Series B" OR "Series C" OR "venture capital" OR "funding round") when:2d') }
    ],
    cloud: [
      { name: "InfoQ", url: "https://feed.infoq.com/" },
      { name: "The New Stack", url: "https://thenewstack.io/feed/" }
    ],
    layoffs: [
      { name: "TechCrunch Layoffs", url: "https://techcrunch.com/tag/layoffs/feed/" }
    ],
    finance: [
      { name: "CNBC Tech", url: "https://www.cnbc.com/id/19854910/device/rss/rss.html" },
      { name: "Yahoo Finance", url: "https://finance.yahoo.com/rss/topstories" }
    ]
  },
  finance: {
    markets: [
      { name: "CNBC", url: "https://www.cnbc.com/id/100003114/device/rss/rss.html" },
      { name: "Yahoo Finance", url: "https://finance.yahoo.com/rss/topstories" },
      { name: "Seeking Alpha", url: "https://seekingalpha.com/market_currents.xml" }
    ],
    forex: [
      { name: "Forex News", url: gn('(forex OR currency OR "exchange rate" OR FX OR "US dollar") when:2d') }
    ],
    bonds: [
      { name: "Bond Market", url: gn('("bond market" OR "treasury yield" OR "bond yield" OR "fixed income") when:2d') }
    ],
    commodities: [
      { name: "Oil & Gas", url: gn('(oil price OR OPEC OR "natural gas" OR pipeline OR LNG) when:2d') },
      { name: "Gold & Metals", url: gn('("gold price" OR "silver price" OR "precious metals" OR "copper price") when:2d') }
    ],
    crypto: [
      { name: "CoinDesk", url: "https://www.coindesk.com/arc/outboundfeeds/rss/" },
      { name: "Cointelegraph", url: "https://cointelegraph.com/rss" }
    ],
    centralbanks: [
      { name: "Federal Reserve", url: "https://www.federalreserve.gov/feeds/press_all.xml" }
    ],
    economic: [
      { name: "Economic Data", url: gn('(CPI OR inflation OR GDP OR "economic data" OR "jobs report") when:2d') }
    ],
    ipo: [
      { name: "IPO News", url: gn('(IPO OR "initial public offering" OR "stock market debut") when:2d') }
    ],
    derivatives: [
      { name: "Options Market", url: gn('("options market" OR "options trading" OR "put call ratio" OR VIX) when:2d') },
      { name: "Futures Trading", url: gn('("futures trading" OR "S&P 500 futures" OR "Nasdaq futures") when:1d') }
    ],
    fintech: [
      { name: "Fintech News", url: gn('(fintech OR "payment technology" OR neobank OR "digital banking") when:3d') },
      { name: "Trading Tech", url: gn('("algorithmic trading" OR "trading platform" OR "quantitative finance") when:7d') },
      { name: "Blockchain Finance", url: gn('("blockchain finance" OR tokenization OR "digital securities" OR CBDC) when:7d') }
    ],
    regulation: [
      { name: "SEC", url: "https://www.sec.gov/news/pressreleases.rss" },
      { name: "Financial Regulation", url: gn("(SEC OR CFTC OR FINRA OR FCA) regulation OR enforcement when:3d") },
      { name: "Banking Rules", url: gn('(Basel OR "capital requirements" OR "banking regulation") when:7d') },
      { name: "Crypto Regulation", url: gn('(crypto regulation OR "digital asset" regulation OR stablecoin regulation) when:7d') }
    ],
    institutional: [
      { name: "Hedge Fund News", url: gn('("hedge fund" OR Bridgewater OR Citadel OR Renaissance) when:7d') },
      { name: "Private Equity", url: gn('("private equity" OR Blackstone OR KKR OR Apollo OR Carlyle) when:3d') },
      { name: "Sovereign Wealth", url: gn('("sovereign wealth fund" OR "pension fund" OR "institutional investor") when:7d') }
    ],
    analysis: [
      { name: "Market Outlook", url: gn('("market outlook" OR "stock market forecast" OR "bull market" OR "bear market") when:3d') },
      { name: "Risk & Volatility", url: gn('(VIX OR "market volatility" OR "risk off" OR "market correction") when:3d') },
      { name: "Bank Research", url: gn('("Goldman Sachs" OR JPMorgan OR "Morgan Stanley") forecast OR outlook when:3d') }
    ],
    gccNews: [
      { name: "Arabian Business", url: gn("site:arabianbusiness.com (Saudi Arabia OR UAE OR GCC) when:7d") },
      { name: "The National", url: gn("site:thenationalnews.com (Abu Dhabi OR UAE OR Saudi) when:7d") },
      { name: "Arab News", url: gn("site:arabnews.com (Saudi Arabia OR investment OR infrastructure) when:7d") },
      { name: "Gulf FDI", url: gn('(PIF OR "DP World" OR Mubadala OR ADNOC OR Masdar OR "ACWA Power") infrastructure when:7d') },
      { name: "Gulf Investments", url: gn('("Saudi Arabia" OR UAE OR "Abu Dhabi") investment infrastructure when:7d') },
      { name: "Vision 2030", url: gn('"Vision 2030" (project OR investment OR announced) when:14d') }
    ]
  },
  happy: {
    positive: [
      { name: "Good News Network", url: "https://www.goodnewsnetwork.org/feed/" },
      { name: "Positive.News", url: "https://www.positive.news/feed/" },
      { name: "Reasons to be Cheerful", url: "https://reasonstobecheerful.world/feed/" },
      { name: "Optimist Daily", url: "https://www.optimistdaily.com/feed/" }
    ],
    science: [
      { name: "ScienceDaily", url: "https://www.sciencedaily.com/rss/all.xml" },
      { name: "Nature News", url: "https://feeds.nature.com/nature/rss/current" },
      { name: "Singularity Hub", url: "https://singularityhub.com/feed/" }
    ]
  }
};
var INTEL_SOURCES = [
  { name: "Defense One", url: "https://www.defenseone.com/rss/all/" },
  { name: "Breaking Defense", url: "https://breakingdefense.com/feed/" },
  { name: "The War Zone", url: "https://www.twz.com/feed" },
  { name: "Defense News", url: "https://www.defensenews.com/arc/outboundfeeds/rss/?outputType=xml" },
  { name: "Military Times", url: "https://www.militarytimes.com/arc/outboundfeeds/rss/?outputType=xml" },
  { name: "Task & Purpose", url: "https://taskandpurpose.com/feed/" },
  { name: "USNI News", url: "https://news.usni.org/feed" },
  { name: "gCaptain", url: "https://gcaptain.com/feed/" },
  { name: "Oryx OSINT", url: "https://www.oryxspioenkop.com/feeds/posts/default?alt=rss" },
  { name: "Foreign Policy", url: "https://foreignpolicy.com/feed/" },
  { name: "Foreign Affairs", url: "https://www.foreignaffairs.com/rss.xml" },
  { name: "Atlantic Council", url: "https://www.atlanticcouncil.org/feed/" },
  { name: "Bellingcat", url: gn("site:bellingcat.com") },
  { name: "Krebs Security", url: "https://krebsonsecurity.com/feed/" },
  { name: "Arms Control Assn", url: gn("site:armscontrol.org") },
  { name: "Bulletin of Atomic Scientists", url: gn("site:thebulletin.org") },
  { name: "FAO News", url: "https://www.fao.org/feeds/fao-newsroom-rss" }
];

// server/worldmonitor/news/v1/_classifier.ts
var CRITICAL_KEYWORDS = {
  "nuclear strike": "military",
  "nuclear attack": "military",
  "nuclear war": "military",
  "invasion": "conflict",
  "declaration of war": "conflict",
  "martial law": "military",
  "coup": "military",
  "coup attempt": "military",
  "genocide": "conflict",
  "ethnic cleansing": "conflict",
  "chemical attack": "terrorism",
  "biological attack": "terrorism",
  "dirty bomb": "terrorism",
  "mass casualty": "conflict",
  "pandemic declared": "health",
  "health emergency": "health",
  "nato article 5": "military",
  "evacuation order": "disaster",
  "meltdown": "disaster",
  "nuclear meltdown": "disaster"
};
var HIGH_KEYWORDS = {
  "war": "conflict",
  "armed conflict": "conflict",
  "airstrike": "conflict",
  "air strike": "conflict",
  "drone strike": "conflict",
  "missile": "military",
  "missile launch": "military",
  "troops deployed": "military",
  "military escalation": "military",
  "bombing": "conflict",
  "casualties": "conflict",
  "hostage": "terrorism",
  "terrorist": "terrorism",
  "terror attack": "terrorism",
  "assassination": "crime",
  "cyber attack": "cyber",
  "ransomware": "cyber",
  "data breach": "cyber",
  "sanctions": "economic",
  "embargo": "economic",
  "earthquake": "disaster",
  "tsunami": "disaster",
  "hurricane": "disaster",
  "typhoon": "disaster"
};
var MEDIUM_KEYWORDS = {
  "protest": "protest",
  "protests": "protest",
  "riot": "protest",
  "riots": "protest",
  "unrest": "protest",
  "demonstration": "protest",
  "strike action": "protest",
  "military exercise": "military",
  "naval exercise": "military",
  "arms deal": "military",
  "weapons sale": "military",
  "diplomatic crisis": "diplomatic",
  "ambassador recalled": "diplomatic",
  "expel diplomats": "diplomatic",
  "trade war": "economic",
  "tariff": "economic",
  "recession": "economic",
  "inflation": "economic",
  "market crash": "economic",
  "flood": "disaster",
  "flooding": "disaster",
  "wildfire": "disaster",
  "volcano": "disaster",
  "eruption": "disaster",
  "outbreak": "health",
  "epidemic": "health",
  "infection spread": "health",
  "oil spill": "environmental",
  "pipeline explosion": "infrastructure",
  "blackout": "infrastructure",
  "power outage": "infrastructure",
  "internet outage": "infrastructure",
  "derailment": "infrastructure"
};
var LOW_KEYWORDS = {
  "election": "diplomatic",
  "vote": "diplomatic",
  "referendum": "diplomatic",
  "summit": "diplomatic",
  "treaty": "diplomatic",
  "agreement": "diplomatic",
  "negotiation": "diplomatic",
  "talks": "diplomatic",
  "peacekeeping": "diplomatic",
  "humanitarian aid": "diplomatic",
  "ceasefire": "diplomatic",
  "peace treaty": "diplomatic",
  "climate change": "environmental",
  "emissions": "environmental",
  "pollution": "environmental",
  "deforestation": "environmental",
  "drought": "environmental",
  "vaccine": "health",
  "vaccination": "health",
  "disease": "health",
  "virus": "health",
  "public health": "health",
  "covid": "health",
  "interest rate": "economic",
  "gdp": "economic",
  "unemployment": "economic",
  "regulation": "economic"
};
var TECH_HIGH_KEYWORDS = {
  "major outage": "infrastructure",
  "service down": "infrastructure",
  "global outage": "infrastructure",
  "zero-day": "cyber",
  "critical vulnerability": "cyber",
  "supply chain attack": "cyber",
  "mass layoff": "economic"
};
var TECH_MEDIUM_KEYWORDS = {
  "outage": "infrastructure",
  "breach": "cyber",
  "hack": "cyber",
  "vulnerability": "cyber",
  "layoff": "economic",
  "layoffs": "economic",
  "antitrust": "economic",
  "monopoly": "economic",
  "ban": "economic",
  "shutdown": "infrastructure"
};
var TECH_LOW_KEYWORDS = {
  "ipo": "economic",
  "funding": "economic",
  "acquisition": "economic",
  "merger": "economic",
  "launch": "tech",
  "release": "tech",
  "update": "tech",
  "partnership": "economic",
  "startup": "tech",
  "ai model": "tech",
  "open source": "tech"
};
var EXCLUSIONS = [
  "protein",
  "couples",
  "relationship",
  "dating",
  "diet",
  "fitness",
  "recipe",
  "cooking",
  "shopping",
  "fashion",
  "celebrity",
  "movie",
  "tv show",
  "sports",
  "game",
  "concert",
  "festival",
  "wedding",
  "vacation",
  "travel tips",
  "life hack",
  "self-care",
  "wellness"
];
var SHORT_KEYWORDS = /* @__PURE__ */ new Set([
  "war",
  "coup",
  "ban",
  "vote",
  "riot",
  "riots",
  "hack",
  "talks",
  "ipo",
  "gdp",
  "virus",
  "disease",
  "flood"
]);
var keywordRegexCache = /* @__PURE__ */ new Map();
function getKeywordRegex(kw) {
  let re = keywordRegexCache.get(kw);
  if (!re) {
    re = SHORT_KEYWORDS.has(kw) ? new RegExp(`\\b${kw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\b`) : new RegExp(kw.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
    keywordRegexCache.set(kw, re);
  }
  return re;
}
function matchKeywords(titleLower, keywords) {
  for (const [kw, cat] of Object.entries(keywords)) {
    if (getKeywordRegex(kw).test(titleLower)) {
      return { keyword: kw, category: cat };
    }
  }
  return null;
}
function classifyByKeyword(title, variant) {
  const lower = title.toLowerCase();
  if (EXCLUSIONS.some((ex) => lower.includes(ex))) {
    return { level: "info", category: "general", confidence: 0.3, source: "keyword" };
  }
  const isTech = variant === "tech";
  let match = matchKeywords(lower, CRITICAL_KEYWORDS);
  if (match) return { level: "critical", category: match.category, confidence: 0.9, source: "keyword" };
  match = matchKeywords(lower, HIGH_KEYWORDS);
  if (match) return { level: "high", category: match.category, confidence: 0.8, source: "keyword" };
  if (isTech) {
    match = matchKeywords(lower, TECH_HIGH_KEYWORDS);
    if (match) return { level: "high", category: match.category, confidence: 0.75, source: "keyword" };
  }
  match = matchKeywords(lower, MEDIUM_KEYWORDS);
  if (match) return { level: "medium", category: match.category, confidence: 0.7, source: "keyword" };
  if (isTech) {
    match = matchKeywords(lower, TECH_MEDIUM_KEYWORDS);
    if (match) return { level: "medium", category: match.category, confidence: 0.65, source: "keyword" };
  }
  match = matchKeywords(lower, LOW_KEYWORDS);
  if (match) return { level: "low", category: match.category, confidence: 0.6, source: "keyword" };
  if (isTech) {
    match = matchKeywords(lower, TECH_LOW_KEYWORDS);
    if (match) return { level: "low", category: match.category, confidence: 0.55, source: "keyword" };
  }
  return { level: "info", category: "general", confidence: 0.3, source: "keyword" };
}

// server/worldmonitor/news/v1/list-feed-digest.ts
var VALID_VARIANTS = /* @__PURE__ */ new Set(["full", "tech", "finance", "happy"]);
var fallbackDigestCache = /* @__PURE__ */ new Map();
var ITEMS_PER_FEED = 5;
var MAX_ITEMS_PER_CATEGORY = 20;
var FEED_TIMEOUT_MS = 8e3;
var OVERALL_DEADLINE_MS = 25e3;
var BATCH_CONCURRENCY2 = 20;
var LEVEL_TO_PROTO = {
  critical: "THREAT_LEVEL_CRITICAL",
  high: "THREAT_LEVEL_HIGH",
  medium: "THREAT_LEVEL_MEDIUM",
  low: "THREAT_LEVEL_LOW",
  info: "THREAT_LEVEL_UNSPECIFIED"
};
async function fetchAndParseRss(feed, variant, signal) {
  const cacheKey2 = `rss:feed:v1:${feed.url}`;
  try {
    const cached = await cachedFetchJson(cacheKey2, 600, async () => {
      const controller = new AbortController();
      const timeout = setTimeout(() => controller.abort(), FEED_TIMEOUT_MS);
      const onAbort = () => controller.abort();
      signal.addEventListener("abort", onAbort, { once: true });
      try {
        const resp = await fetch(feed.url, {
          headers: {
            "User-Agent": CHROME_UA,
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
            "Accept-Language": "en-US,en;q=0.9"
          },
          signal: controller.signal
        });
        if (!resp.ok) return null;
        const text = await resp.text();
        return parseRssXml(text, feed, variant);
      } finally {
        clearTimeout(timeout);
        signal.removeEventListener("abort", onAbort);
      }
    });
    return cached ?? [];
  } catch {
    return [];
  }
}
function parseRssXml(xml, feed, variant) {
  const items = [];
  const itemRegex = /<item[\s>]([\s\S]*?)<\/item>/gi;
  const entryRegex = /<entry[\s>]([\s\S]*?)<\/entry>/gi;
  let matches = [...xml.matchAll(itemRegex)];
  const isAtom = matches.length === 0;
  if (isAtom) matches = [...xml.matchAll(entryRegex)];
  for (const match of matches.slice(0, ITEMS_PER_FEED)) {
    const block = match[1];
    const title = extractTag(block, "title");
    if (!title) continue;
    let link;
    if (isAtom) {
      const hrefMatch = block.match(/<link[^>]+href=["']([^"']+)["']/);
      link = hrefMatch?.[1] ?? "";
    } else {
      link = extractTag(block, "link");
    }
    const pubDateStr = isAtom ? extractTag(block, "published") || extractTag(block, "updated") : extractTag(block, "pubDate");
    const parsedDate = pubDateStr ? new Date(pubDateStr) : /* @__PURE__ */ new Date();
    const publishedAt = Number.isNaN(parsedDate.getTime()) ? Date.now() : parsedDate.getTime();
    const threat = classifyByKeyword(title, variant);
    const isAlert = threat.level === "critical" || threat.level === "high";
    items.push({
      source: feed.name,
      title,
      link,
      publishedAt,
      isAlert,
      level: threat.level,
      category: threat.category,
      confidence: threat.confidence,
      classSource: "keyword"
    });
  }
  return items.length > 0 ? items : null;
}
var TAG_REGEX_CACHE = /* @__PURE__ */ new Map();
var KNOWN_TAGS = ["title", "link", "pubDate", "published", "updated"];
for (const tag of KNOWN_TAGS) {
  TAG_REGEX_CACHE.set(tag, {
    cdata: new RegExp(`<${tag}[^>]*>\\s*<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>\\s*<\\/${tag}>`, "i"),
    plain: new RegExp(`<${tag}[^>]*>([^<]*)<\\/${tag}>`, "i")
  });
}
function extractTag(xml, tag) {
  const cached = TAG_REGEX_CACHE.get(tag);
  const cdataRe = cached?.cdata ?? new RegExp(`<${tag}[^>]*>\\s*<!\\[CDATA\\[([\\s\\S]*?)\\]\\]>\\s*<\\/${tag}>`, "i");
  const plainRe = cached?.plain ?? new RegExp(`<${tag}[^>]*>([^<]*)<\\/${tag}>`, "i");
  const cdataMatch = xml.match(cdataRe);
  if (cdataMatch) return cdataMatch[1].trim();
  const match = xml.match(plainRe);
  return match ? decodeXmlEntities(match[1].trim()) : "";
}
function decodeXmlEntities(s) {
  return s.replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&quot;/g, '"').replace(/&apos;/g, "'").replace(/&#(\d+);/g, (_, n) => String.fromCharCode(Number(n))).replace(/&#x([0-9a-fA-F]+);/g, (_, n) => String.fromCharCode(parseInt(n, 16)));
}
function toProtoItem(item) {
  return {
    source: item.source,
    title: item.title,
    link: item.link,
    publishedAt: item.publishedAt,
    isAlert: item.isAlert,
    threat: {
      level: LEVEL_TO_PROTO[item.level],
      category: item.category,
      confidence: item.confidence,
      source: item.classSource
    },
    locationName: ""
  };
}
async function listFeedDigest(_ctx, req) {
  const variant = VALID_VARIANTS.has(req.variant) ? req.variant : "full";
  const lang = req.lang || "en";
  const digestCacheKey = `news:digest:v1:${variant}:${lang}`;
  const fallbackKey = `${variant}:${lang}`;
  try {
    const cached = await cachedFetchJson(digestCacheKey, 900, async () => {
      return buildDigest(variant, lang);
    });
    if (cached) {
      if (fallbackDigestCache.size > 50) fallbackDigestCache.clear();
      fallbackDigestCache.set(fallbackKey, { data: cached, ts: Date.now() });
    }
    return cached ?? fallbackDigestCache.get(fallbackKey)?.data ?? { categories: {}, feedStatuses: {}, generatedAt: (/* @__PURE__ */ new Date()).toISOString() };
  } catch {
    return fallbackDigestCache.get(fallbackKey)?.data ?? { categories: {}, feedStatuses: {}, generatedAt: (/* @__PURE__ */ new Date()).toISOString() };
  }
}
async function buildDigest(variant, lang) {
  const feedsByCategory = VARIANT_FEEDS[variant] ?? {};
  const feedStatuses = {};
  const categories = {};
  const deadlineController = new AbortController();
  const deadlineTimeout = setTimeout(() => deadlineController.abort(), OVERALL_DEADLINE_MS);
  try {
    const allEntries = [];
    for (const [category, feeds] of Object.entries(feedsByCategory)) {
      const filtered = feeds.filter((f) => !f.lang || f.lang === lang);
      for (const feed of filtered) {
        allEntries.push({ category, feed });
      }
    }
    if (variant === "full") {
      const filteredIntel = INTEL_SOURCES.filter((f) => !f.lang || f.lang === lang);
      for (const feed of filteredIntel) {
        allEntries.push({ category: "intel", feed });
      }
    }
    const results = /* @__PURE__ */ new Map();
    for (let i = 0; i < allEntries.length; i += BATCH_CONCURRENCY2) {
      if (deadlineController.signal.aborted) break;
      const batch = allEntries.slice(i, i + BATCH_CONCURRENCY2);
      const settled = await Promise.allSettled(
        batch.map(async ({ category, feed }) => {
          const items = await fetchAndParseRss(feed, variant, deadlineController.signal);
          feedStatuses[feed.name] = items.length > 0 ? "ok" : "empty";
          return { category, items };
        })
      );
      for (const result of settled) {
        if (result.status === "fulfilled") {
          const { category, items } = result.value;
          const existing = results.get(category) ?? [];
          existing.push(...items);
          results.set(category, existing);
        }
      }
    }
    for (const entry of allEntries) {
      if (!(entry.feed.name in feedStatuses)) {
        feedStatuses[entry.feed.name] = "timeout";
      }
    }
    for (const [category, items] of results) {
      items.sort((a, b) => b.publishedAt - a.publishedAt);
      categories[category] = {
        items: items.slice(0, MAX_ITEMS_PER_CATEGORY).map(toProtoItem)
      };
    }
    return {
      categories,
      feedStatuses,
      generatedAt: (/* @__PURE__ */ new Date()).toISOString()
    };
  } finally {
    clearTimeout(deadlineTimeout);
  }
}

// server/worldmonitor/news/v1/handler.ts
var newsHandler = {
  summarizeArticle,
  listFeedDigest
};

// src/generated/server/worldmonitor/intelligence/v1/service_server.ts
var ValidationError16 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createIntelligenceServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/intelligence/v1/get-risk-scores",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            region: params.get("region") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getRiskScores", body);
            if (bodyViolations) {
              throw new ValidationError16(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getRiskScores(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError16) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/intelligence/v1/get-pizzint-status",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            includeGdelt: params.get("include_gdelt") === "true"
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getPizzintStatus", body);
            if (bodyViolations) {
              throw new ValidationError16(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getPizzintStatus(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError16) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "POST",
      path: "/api/intelligence/v1/classify-event",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = await req.json();
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("classifyEvent", body);
            if (bodyViolations) {
              throw new ValidationError16(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.classifyEvent(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError16) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/intelligence/v1/get-country-intel-brief",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            countryCode: params.get("country_code") ?? ""
          };
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getCountryIntelBrief(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError16) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/intelligence/v1/search-gdelt-documents",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            query: params.get("query") ?? "",
            maxRecords: Number(params.get("max_records") ?? "0"),
            timespan: params.get("timespan") ?? "",
            toneFilter: params.get("tone_filter") ?? "",
            sort: params.get("sort") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("searchGdeltDocuments", body);
            if (bodyViolations) {
              throw new ValidationError16(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.searchGdeltDocuments(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError16) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "POST",
      path: "/api/intelligence/v1/deduct-situation",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = await req.json();
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("deductSituation", body);
            if (bodyViolations) {
              throw new ValidationError16(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.deductSituation(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError16) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/intelligence/v1/_shared.ts
var UPSTREAM_TIMEOUT_MS4 = 3e4;
var GROQ_API_URL = "https://api.groq.com/openai/v1/chat/completions";
var GROQ_MODEL = "llama-3.1-8b-instant";
var TIER1_COUNTRIES = {
  US: "United States",
  RU: "Russia",
  CN: "China",
  UA: "Ukraine",
  IR: "Iran",
  IL: "Israel",
  TW: "Taiwan",
  KP: "North Korea",
  SA: "Saudi Arabia",
  TR: "Turkey",
  PL: "Poland",
  DE: "Germany",
  FR: "France",
  GB: "United Kingdom",
  IN: "India",
  PK: "Pakistan",
  SY: "Syria",
  YE: "Yemen",
  MM: "Myanmar",
  VE: "Venezuela"
};

// server/worldmonitor/intelligence/v1/get-risk-scores.ts
var BASELINE_RISK = {
  US: 5,
  RU: 35,
  CN: 25,
  UA: 50,
  IR: 40,
  IL: 45,
  TW: 30,
  KP: 45,
  SA: 20,
  TR: 25,
  PL: 10,
  DE: 5,
  FR: 10,
  GB: 5,
  IN: 20,
  PK: 35,
  SY: 50,
  YE: 50,
  MM: 45,
  VE: 40
};
var EVENT_MULTIPLIER = {
  US: 0.3,
  RU: 2,
  CN: 2.5,
  UA: 0.8,
  IR: 2,
  IL: 0.7,
  TW: 1.5,
  KP: 3,
  SA: 2,
  TR: 1.2,
  PL: 0.8,
  DE: 0.5,
  FR: 0.6,
  GB: 0.5,
  IN: 0.8,
  PK: 1.5,
  SY: 0.7,
  YE: 0.7,
  MM: 1.8,
  VE: 1.8
};
var COUNTRY_KEYWORDS = {
  US: ["united states", "usa", "america", "washington", "biden", "trump", "pentagon"],
  RU: ["russia", "moscow", "kremlin", "putin"],
  CN: ["china", "beijing", "xi jinping", "prc"],
  UA: ["ukraine", "kyiv", "zelensky", "donbas"],
  IR: ["iran", "tehran", "khamenei", "irgc"],
  IL: ["israel", "tel aviv", "netanyahu", "idf", "gaza"],
  TW: ["taiwan", "taipei"],
  KP: ["north korea", "pyongyang", "kim jong"],
  SA: ["saudi arabia", "riyadh"],
  TR: ["turkey", "ankara", "erdogan"],
  PL: ["poland", "warsaw"],
  DE: ["germany", "berlin"],
  FR: ["france", "paris", "macron"],
  GB: ["britain", "uk", "london"],
  IN: ["india", "delhi", "modi"],
  PK: ["pakistan", "islamabad"],
  SY: ["syria", "damascus"],
  YE: ["yemen", "sanaa", "houthi"],
  MM: ["myanmar", "burma"],
  VE: ["venezuela", "caracas", "maduro"]
};
function normalizeCountryName(text) {
  const lower = text.toLowerCase();
  for (const [code, keywords] of Object.entries(COUNTRY_KEYWORDS)) {
    if (keywords.some((kw) => lower.includes(kw))) return code;
  }
  return null;
}
async function fetchACLEDProtests() {
  const endDate = (/* @__PURE__ */ new Date()).toISOString().split("T")[0];
  const startDate = new Date(Date.now() - 7 * 24 * 60 * 60 * 1e3).toISOString().split("T")[0];
  const raw = await fetchAcledCached({
    eventTypes: "Protests|Riots",
    startDate,
    endDate
  });
  return raw.map((e) => ({ country: e.country || "", event_type: e.event_type || "" }));
}
function computeCIIScores(protests) {
  const countryEvents = /* @__PURE__ */ new Map();
  for (const event of protests) {
    const code = normalizeCountryName(event.country);
    if (code && TIER1_COUNTRIES[code]) {
      const count = countryEvents.get(code) || { protests: 0, riots: 0 };
      if (event.event_type === "Riots") count.riots++;
      else count.protests++;
      countryEvents.set(code, count);
    }
  }
  const scores = [];
  for (const [code, _name] of Object.entries(TIER1_COUNTRIES)) {
    const events = countryEvents.get(code) || { protests: 0, riots: 0 };
    const baseline = BASELINE_RISK[code] || 20;
    const multiplier = EVENT_MULTIPLIER[code] || 1;
    const unrest = Math.min(100, Math.round((events.protests + events.riots * 2) * multiplier * 2));
    const security = Math.min(100, baseline + events.riots * multiplier * 5);
    const information = Math.min(100, (events.protests + events.riots) * multiplier * 3);
    const composite = Math.min(100, Math.round(baseline + (unrest * 0.4 + security * 0.35 + information * 0.25) * 0.5));
    scores.push({
      region: code,
      staticBaseline: baseline,
      dynamicScore: composite - baseline,
      combinedScore: composite,
      trend: "TREND_DIRECTION_STABLE",
      components: {
        newsActivity: information,
        ciiContribution: unrest,
        geoConvergence: 0,
        militaryActivity: 0
      },
      computedAt: Date.now()
    });
  }
  scores.sort((a, b) => b.combinedScore - a.combinedScore);
  return scores;
}
function computeStrategicRisks(ciiScores) {
  const top5 = ciiScores.slice(0, 5);
  const weights = top5.map((_, i) => 1 - i * 0.15);
  const totalWeight = weights.reduce((sum, w) => sum + w, 0);
  const weightedSum = top5.reduce((sum, s, i) => sum + s.combinedScore * weights[i], 0);
  const overallScore = Math.min(100, Math.round(weightedSum / totalWeight * 0.7 + 15));
  return [
    {
      region: "global",
      level: overallScore >= 70 ? "SEVERITY_LEVEL_HIGH" : overallScore >= 40 ? "SEVERITY_LEVEL_MEDIUM" : "SEVERITY_LEVEL_LOW",
      score: overallScore,
      factors: top5.map((s) => s.region),
      trend: "TREND_DIRECTION_STABLE"
    }
  ];
}
var RISK_CACHE_KEY = "risk:scores:sebuf:v1";
var RISK_STALE_CACHE_KEY = "risk:scores:sebuf:stale:v1";
var RISK_CACHE_TTL = 600;
var RISK_STALE_TTL = 3600;
async function getRiskScores(_ctx, _req) {
  try {
    const result = await cachedFetchJson(
      RISK_CACHE_KEY,
      RISK_CACHE_TTL,
      async () => {
        const protests = await fetchACLEDProtests();
        const ciiScores2 = computeCIIScores(protests);
        const strategicRisks = computeStrategicRisks(ciiScores2);
        const r = { ciiScores: ciiScores2, strategicRisks };
        await setCachedJson(RISK_STALE_CACHE_KEY, r, RISK_STALE_TTL).catch(() => {
        });
        return r;
      }
    );
    if (result) return result;
  } catch {
  }
  const stale = await getCachedJson(RISK_STALE_CACHE_KEY);
  if (stale) return stale;
  const ciiScores = computeCIIScores([]);
  return { ciiScores, strategicRisks: computeStrategicRisks(ciiScores) };
}

// server/worldmonitor/intelligence/v1/get-pizzint-status.ts
var REDIS_CACHE_KEY30 = "intel:pizzint:v1";
var REDIS_CACHE_TTL30 = 600;
var PIZZINT_API = "https://www.pizzint.watch/api/dashboard-data";
var GDELT_BATCH_API = "https://www.pizzint.watch/api/gdelt/batch";
var DEFAULT_GDELT_PAIRS = "usa_russia,russia_ukraine,usa_china,china_taiwan,usa_iran,usa_venezuela";
async function getPizzintStatus(_ctx, req) {
  const cacheKey2 = `${REDIS_CACHE_KEY30}:${req.includeGdelt ? "gdelt" : "base"}`;
  let result = null;
  try {
    result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL30, async () => {
      let pizzint;
      try {
        const resp = await fetch(PIZZINT_API, {
          headers: { Accept: "application/json", "User-Agent": CHROME_UA },
          signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS4)
        });
        if (!resp.ok) throw new Error(`PizzINT API returned ${resp.status}`);
        const raw = await resp.json();
        if (raw.success && raw.data) {
          const locations = raw.data.map((d) => ({
            placeId: d.place_id,
            name: d.name,
            address: d.address,
            currentPopularity: d.current_popularity,
            percentageOfUsual: d.percentage_of_usual ?? 0,
            isSpike: d.is_spike,
            spikeMagnitude: d.spike_magnitude ?? 0,
            dataSource: d.data_source,
            recordedAt: d.recorded_at,
            dataFreshness: d.data_freshness === "fresh" ? "DATA_FRESHNESS_FRESH" : "DATA_FRESHNESS_STALE",
            isClosedNow: d.is_closed_now ?? false,
            lat: d.lat ?? 0,
            lng: d.lng ?? 0
          }));
          const openLocations = locations.filter((l) => !l.isClosedNow);
          const activeSpikes = locations.filter((l) => l.isSpike).length;
          const avgPop = openLocations.length > 0 ? openLocations.reduce((s, l) => s + l.currentPopularity, 0) / openLocations.length : 0;
          let adjusted = avgPop;
          if (activeSpikes > 0) adjusted += activeSpikes * 10;
          adjusted = Math.min(100, adjusted);
          let defconLevel = 5;
          let defconLabel = "Normal Activity";
          if (adjusted >= 85) {
            defconLevel = 1;
            defconLabel = "Maximum Activity";
          } else if (adjusted >= 70) {
            defconLevel = 2;
            defconLabel = "High Activity";
          } else if (adjusted >= 50) {
            defconLevel = 3;
            defconLabel = "Elevated Activity";
          } else if (adjusted >= 25) {
            defconLevel = 4;
            defconLabel = "Above Normal";
          }
          const hasFresh = locations.some((l) => l.dataFreshness === "DATA_FRESHNESS_FRESH");
          pizzint = {
            defconLevel,
            defconLabel,
            aggregateActivity: Math.round(avgPop),
            activeSpikes,
            locationsMonitored: locations.length,
            locationsOpen: openLocations.length,
            updatedAt: Date.now(),
            dataFreshness: hasFresh ? "DATA_FRESHNESS_FRESH" : "DATA_FRESHNESS_STALE",
            locations
          };
        }
      } catch (_) {
      }
      let tensionPairs = [];
      if (req.includeGdelt) {
        try {
          const url = `${GDELT_BATCH_API}?pairs=${encodeURIComponent(DEFAULT_GDELT_PAIRS)}&method=gpr`;
          const resp = await fetch(url, {
            headers: { Accept: "application/json", "User-Agent": CHROME_UA },
            signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS4)
          });
          if (resp.ok) {
            const raw = await resp.json();
            tensionPairs = Object.entries(raw).map(([pairKey, dataPoints]) => {
              const countries = pairKey.split("_");
              const latest = dataPoints[dataPoints.length - 1];
              const prev = dataPoints.length > 1 ? dataPoints[dataPoints.length - 2] : latest;
              const change = prev.v > 0 ? (latest.v - prev.v) / prev.v * 100 : 0;
              const trend = change > 5 ? "TREND_DIRECTION_RISING" : change < -5 ? "TREND_DIRECTION_FALLING" : "TREND_DIRECTION_STABLE";
              return {
                id: pairKey,
                countries,
                label: countries.map((c) => c.toUpperCase()).join(" - "),
                score: latest?.v ?? 0,
                trend,
                changePercent: Math.round(change * 10) / 10,
                region: "global"
              };
            });
          }
        } catch {
        }
      }
      if (!pizzint) return null;
      return { pizzint, tensionPairs };
    });
  } catch {
    return { pizzint: void 0, tensionPairs: [] };
  }
  return result || { pizzint: void 0, tensionPairs: [] };
}

// server/worldmonitor/intelligence/v1/classify-event.ts
var CLASSIFY_CACHE_TTL = 86400;
var VALID_LEVELS = ["critical", "high", "medium", "low", "info"];
var VALID_CATEGORIES = [
  "conflict",
  "protest",
  "disaster",
  "diplomatic",
  "economic",
  "terrorism",
  "cyber",
  "health",
  "environmental",
  "military",
  "crime",
  "infrastructure",
  "tech",
  "general"
];
function mapLevelToSeverity(level) {
  if (level === "critical" || level === "high") return "SEVERITY_LEVEL_HIGH";
  if (level === "medium") return "SEVERITY_LEVEL_MEDIUM";
  return "SEVERITY_LEVEL_LOW";
}
async function classifyEvent(_ctx, req) {
  const apiKey = process.env.GROQ_API_KEY;
  if (!apiKey) return { classification: void 0 };
  const MAX_TITLE_LEN = 500;
  const title = typeof req.title === "string" ? req.title.slice(0, MAX_TITLE_LEN) : "";
  if (!title) return { classification: void 0 };
  const cacheKey2 = `classify:sebuf:v1:${hashString(title.toLowerCase())}`;
  let cached = null;
  try {
    cached = await cachedFetchJson(
      cacheKey2,
      CLASSIFY_CACHE_TTL,
      async () => {
        try {
          const systemPrompt = `You classify news headlines into threat level and category. Return ONLY valid JSON, no other text.

Levels: critical, high, medium, low, info
Categories: conflict, protest, disaster, diplomatic, economic, terrorism, cyber, health, environmental, military, crime, infrastructure, tech, general

Focus: geopolitical events, conflicts, disasters, diplomacy. Classify by real-world severity and impact.

Return: {"level":"...","category":"..."}`;
          const resp = await fetch(GROQ_API_URL, {
            method: "POST",
            headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json", "User-Agent": CHROME_UA },
            body: JSON.stringify({
              model: GROQ_MODEL,
              messages: [
                { role: "system", content: systemPrompt },
                { role: "user", content: title }
              ],
              temperature: 0,
              max_tokens: 50
            }),
            signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS4)
          });
          if (!resp.ok) return null;
          const data = await resp.json();
          const raw = data.choices?.[0]?.message?.content?.trim();
          if (!raw) return null;
          let parsed;
          try {
            parsed = JSON.parse(raw);
          } catch {
            return null;
          }
          const level = VALID_LEVELS.includes(parsed.level ?? "") ? parsed.level : null;
          const category = VALID_CATEGORIES.includes(parsed.category ?? "") ? parsed.category : null;
          if (!level || !category) return null;
          return { level, category, timestamp: Date.now() };
        } catch {
          return null;
        }
      }
    );
  } catch {
    return { classification: void 0 };
  }
  if (!cached?.level || !cached?.category) return { classification: void 0 };
  return {
    classification: {
      category: cached.category,
      subcategory: cached.level,
      severity: mapLevelToSeverity(cached.level),
      confidence: 0.9,
      analysis: "",
      entities: []
    }
  };
}

// server/worldmonitor/intelligence/v1/get-country-intel-brief.ts
var INTEL_CACHE_TTL = 7200;
async function getCountryIntelBrief(ctx, req) {
  const empty = {
    countryCode: req.countryCode,
    countryName: "",
    brief: "",
    model: GROQ_MODEL,
    generatedAt: Date.now()
  };
  if (!req.countryCode) return empty;
  const apiKey = process.env.GROQ_API_KEY;
  if (!apiKey) return empty;
  let contextSnapshot = "";
  try {
    const url = new URL(ctx.request.url);
    contextSnapshot = (url.searchParams.get("context") || "").trim().slice(0, 4e3);
  } catch {
    contextSnapshot = "";
  }
  const contextHash = contextSnapshot ? hashString(contextSnapshot) : "base";
  const cacheKey2 = `ci-sebuf:v2:${req.countryCode}:${contextHash}`;
  const countryName = TIER1_COUNTRIES[req.countryCode] || req.countryCode;
  const dateStr = (/* @__PURE__ */ new Date()).toISOString().split("T")[0];
  const systemPrompt = `You are a senior intelligence analyst providing comprehensive country situation briefs. Current date: ${dateStr}. Provide geopolitical context appropriate for the current date.

Write a concise intelligence brief for the requested country covering:
1. Current Situation - what is happening right now
2. Military & Security Posture
3. Key Risk Factors
4. Regional Context
5. Outlook & Watch Items

Rules:
- Be specific and analytical
- 4-5 paragraphs, 250-350 words
- No speculation beyond what data supports
- Use plain language, not jargon
- If a context snapshot is provided, explicitly reflect each non-zero signal category in the brief`;
  let result = null;
  try {
    result = await cachedFetchJson(cacheKey2, INTEL_CACHE_TTL, async () => {
      try {
        const userPromptParts = [
          `Country: ${countryName} (${req.countryCode})`
        ];
        if (contextSnapshot) {
          userPromptParts.push(`Context snapshot:
${contextSnapshot}`);
        }
        const resp = await fetch(GROQ_API_URL, {
          method: "POST",
          headers: { Authorization: `Bearer ${apiKey}`, "Content-Type": "application/json", "User-Agent": CHROME_UA },
          body: JSON.stringify({
            model: GROQ_MODEL,
            messages: [
              { role: "system", content: systemPrompt },
              { role: "user", content: userPromptParts.join("\n\n") }
            ],
            temperature: 0.4,
            max_tokens: 900
          }),
          signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS4)
        });
        if (!resp.ok) return null;
        const data = await resp.json();
        const brief = data.choices?.[0]?.message?.content?.trim() || "";
        if (!brief) return null;
        return {
          countryCode: req.countryCode,
          countryName,
          brief,
          model: GROQ_MODEL,
          generatedAt: Date.now()
        };
      } catch {
        return null;
      }
    });
  } catch {
    return empty;
  }
  return result || empty;
}

// server/worldmonitor/intelligence/v1/search-gdelt-documents.ts
var REDIS_CACHE_KEY31 = "intel:gdelt-docs:v1";
var REDIS_CACHE_TTL31 = 600;
var GDELT_MAX_RECORDS = 20;
var GDELT_DEFAULT_RECORDS = 10;
var GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc";
async function searchGdeltDocuments(_ctx, req) {
  let query = req.query;
  if (!query || query.length < 2) {
    return { articles: [], query: query || "", error: "Query parameter required (min 2 characters)" };
  }
  if (req.toneFilter) {
    query = `${query} ${req.toneFilter}`;
  }
  const maxRecords = Math.min(
    req.maxRecords > 0 ? req.maxRecords : GDELT_DEFAULT_RECORDS,
    GDELT_MAX_RECORDS
  );
  const timespan = req.timespan || "72h";
  try {
    const cacheKey2 = `${REDIS_CACHE_KEY31}:${query}:${timespan}:${maxRecords}`;
    const result = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL31,
      async () => {
        const gdeltUrl = new URL(GDELT_DOC_API);
        gdeltUrl.searchParams.set("query", query);
        gdeltUrl.searchParams.set("mode", "artlist");
        gdeltUrl.searchParams.set("maxrecords", maxRecords.toString());
        gdeltUrl.searchParams.set("format", "json");
        gdeltUrl.searchParams.set("sort", req.sort || "date");
        gdeltUrl.searchParams.set("timespan", timespan);
        const response = await fetch(gdeltUrl.toString(), {
          headers: { "User-Agent": CHROME_UA },
          signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS4)
        });
        if (!response.ok) {
          throw new Error(`GDELT returned ${response.status}`);
        }
        const data = await response.json();
        const articles = (data.articles || []).map((article) => ({
          title: article.title || "",
          url: article.url || "",
          source: article.domain || article.source?.domain || "",
          date: article.seendate || "",
          image: article.socialimage || "",
          language: article.language || "",
          tone: typeof article.tone === "number" ? article.tone : 0
        }));
        if (articles.length === 0) return null;
        return { articles, query, error: "" };
      }
    );
    return result || { articles: [], query, error: "" };
  } catch (error) {
    return {
      articles: [],
      query,
      error: error instanceof Error ? error.message : "Unknown error"
    };
  }
}

// server/worldmonitor/intelligence/v1/deduct-situation.ts
var DEDUCT_TIMEOUT_MS = 12e4;
var DEDUCT_CACHE_TTL = 3600;
var DEFAULT_API_URL = "https://api.groq.com/openai/v1/chat/completions";
var DEFAULT_MODEL = "llama-3.1-8b-instant";
async function deductSituation(_ctx, req) {
  const apiKey = process.env.LLM_API_KEY || process.env.GROQ_API_KEY;
  const apiUrl = process.env.LLM_API_URL || DEFAULT_API_URL;
  const model = process.env.LLM_MODEL || DEFAULT_MODEL;
  if (!apiKey) {
    return { analysis: "", model: "", provider: "skipped" };
  }
  const MAX_QUERY_LEN = 500;
  const MAX_GEO_LEN = 2e3;
  const query = typeof req.query === "string" ? req.query.slice(0, MAX_QUERY_LEN).trim() : "";
  const geoContext = typeof req.geoContext === "string" ? req.geoContext.slice(0, MAX_GEO_LEN).trim() : "";
  if (!query) return { analysis: "", model: "", provider: "skipped" };
  const cacheKey2 = `deduct:situation:v1:${hashString(query.toLowerCase() + "|" + geoContext.toLowerCase())}`;
  const cached = await cachedFetchJson(
    cacheKey2,
    DEDUCT_CACHE_TTL,
    async () => {
      try {
        const systemPrompt = `You are a senior geopolitical intelligence analyst and forecaster.
Your task is to DEDUCT the situation in a near timeline (e.g. 24 hours to a few months) based on the user's query.
- Use any provided geographic or intelligence context.
- Be highly analytical, pragmatic, and objective.
- Identify the most likely outcomes, timelines, and second-order impacts.
- Do NOT use typical AI preambles (e.g., "Here is the deduction", "Let me see").
- Format your response in clean markdown with concise bullet points where appropriate.`;
        let userPrompt = query;
        if (geoContext) {
          userPrompt += `

### Current Intelligence Context
${geoContext}`;
        }
        const resp = await fetch(apiUrl, {
          method: "POST",
          headers: {
            Authorization: `Bearer ${apiKey}`,
            "Content-Type": "application/json",
            "User-Agent": CHROME_UA
          },
          body: JSON.stringify({
            model,
            messages: [
              { role: "system", content: systemPrompt },
              { role: "user", content: userPrompt }
            ],
            temperature: 0.3,
            max_tokens: 1500
          }),
          signal: AbortSignal.timeout(DEDUCT_TIMEOUT_MS)
        });
        if (!resp.ok) return null;
        const data = await resp.json();
        const firstChoice = data.choices?.[0];
        const content = firstChoice?.message?.content?.trim();
        const reasoning = firstChoice?.message?.reasoning?.trim();
        let raw = content || reasoning;
        if (!raw) return null;
        raw = raw.replace(/<think>[\s\S]*?<\/think>/gi, "").trim();
        return { analysis: raw, model, provider: "groq" };
      } catch (err) {
        console.error("[DeductSituation] Error calling LLM:", err);
        return null;
      }
    }
  );
  if (!cached?.analysis) {
    return { analysis: "", model: "", provider: "error" };
  }
  return {
    analysis: cached.analysis,
    model: cached.model,
    provider: cached.provider
  };
}

// server/worldmonitor/intelligence/v1/handler.ts
var intelligenceHandler = {
  getRiskScores,
  getPizzintStatus,
  classifyEvent,
  getCountryIntelBrief,
  searchGdeltDocuments,
  deductSituation
};

// src/generated/server/worldmonitor/military/v1/service_server.ts
var ValidationError17 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createMilitaryServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/military/v1/list-military-flights",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            pageSize: Number(params.get("page_size") ?? "0"),
            cursor: params.get("cursor") ?? "",
            neLat: Number(params.get("ne_lat") ?? "0"),
            neLon: Number(params.get("ne_lon") ?? "0"),
            swLat: Number(params.get("sw_lat") ?? "0"),
            swLon: Number(params.get("sw_lon") ?? "0"),
            operator: params.get("operator") ?? "",
            aircraftType: params.get("aircraft_type") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listMilitaryFlights", body);
            if (bodyViolations) {
              throw new ValidationError17(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listMilitaryFlights(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError17) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/military/v1/get-theater-posture",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            theater: params.get("theater") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getTheaterPosture", body);
            if (bodyViolations) {
              throw new ValidationError17(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getTheaterPosture(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError17) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/military/v1/get-aircraft-details",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            icao24: params.get("icao24") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getAircraftDetails", body);
            if (bodyViolations) {
              throw new ValidationError17(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getAircraftDetails(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError17) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "POST",
      path: "/api/military/v1/get-aircraft-details-batch",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = await req.json();
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getAircraftDetailsBatch", body);
            if (bodyViolations) {
              throw new ValidationError17(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getAircraftDetailsBatch(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError17) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/military/v1/get-wingbits-status",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getWingbitsStatus(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError17) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/military/v1/get-usni-fleet-report",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            forceRefresh: params.get("force_refresh") === "true"
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getUSNIFleetReport", body);
            if (bodyViolations) {
              throw new ValidationError17(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getUSNIFleetReport(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError17) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/military/v1/list-military-bases",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            neLat: Number(params.get("ne_lat") ?? "0"),
            neLon: Number(params.get("ne_lon") ?? "0"),
            swLat: Number(params.get("sw_lat") ?? "0"),
            swLon: Number(params.get("sw_lon") ?? "0"),
            zoom: Number(params.get("zoom") ?? "0"),
            type: params.get("type") ?? "",
            kind: params.get("kind") ?? "",
            country: params.get("country") ?? ""
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("listMilitaryBases", body);
            if (bodyViolations) {
              throw new ValidationError17(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listMilitaryBases(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError17) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/military/v1/_shared.ts
var _HEX_PACKED = "473c0b800e89507fa07434c6704f803b769b0c404b87c8443b768648d8a4505f6743c91b49849187c8493b768387c8483b77ceae292b3b766d7a42a84a3613738bf0507f897a42a73b75ae152af587c8457585d43b75e987c825152bebe8065fae69d4468103497c9548088048da433b766f3b75bf4a35ceae6ce73b9bdf146634e80699152bdf44ed82505f784a35ad3ac4214850a7afca1e3b77d63b7f9f4682ef731bcdaf4057683251e400d1ae6bd4e49560ae81a54060e5ae5d1fae5d264b7fb87a44107a4940ae27a50101a90101970101aa0200a30200c4ae1860ae52e5ae52ff04c03004c1a67585d18014758015104681577cf8877cf8d87cf8dd7cf8ef7cf9a77cf9d07cf9e27cfa0b7cfa117cfa3f7cfa437cfa62ae6db4ae73e8ae744dae747bae748fae749fae74b8ae74c3ae74e2ae87ed4a35ea4a360cae525ee499243b7b6e3b9bf6457c32457c3300601506439806a25f06a2930a400a0a403d0ac0ac0ac8f60d05d90d81a5140530151d0c152886152894152afe152b68152c041533da155376194e9732001c32003633fde133fe7833ffc73422d63430833474163530563532c53553153571cd3aaad23aab053aab0f3aab443aab5f3aabbe3aabc53aac1a3b76483b76573b76763b77843b77f93b7b663b9bd63e9daf3ea6533eaed63f45203f765a3f841b3fb113400ee0400ee443c15343c2ae43c31743c32643c36443c39543c45843c55443c55c43c5df43c67143c70b43c79c43c8b743c8ea43c8f643c90843c933447d1d447d3b447d50447d52447d5344f18344f66845f42445f43145f43545f439467890477fca477fcc477fd447812848042f48047c48047f48080448080d48085848085d48086748087a48088b48d8ac48d920497c10497c2a497c484984324a82f84a83174b7f5b4b7f634b7f734b7fab4b7fbb4b7fd04b7fd44b7fd74b82024b82174c550f4ca330505f765080665111217061f671028871028c71f01271f902738a007434cb76e31376e72276e72779c23c7a42197a425c7a49557a49df7b70127cf3da7cf83d7cf8567cf85a7cf8687cf86a7cf886800279800325800e14800e4187c40387c40f87c83687cc3f87cc408804068804098805a3881405881b6a881b81881ba2881bc68840068880bc8953f28967dd896c21896c22896c2d8a01338a05e58a073c8a073e8a08438a09338a4501a82420abe1e9adfc60adfc63adfcc1adfcd6adfce2adfd01adfd89adfdc1adfdd6adfe72adfe78adfea3adfea9adff06adffbaadffc4adfff1ae000cae001eae0022ae0035ae0044ae0054ae0062ae006dae007eae0089ae009dae00a7ae0159ae018aae01c0ae01f3ae01f4ae022dae0259ae0266ae02c6ae02efae0374ae037cae0381ae03d3ae03d7ae0404ae0408ae040fae0449ae0451ae045cae046dae0479ae04d8ae04feae056eae057cae0581ae0583ae05d1ae0610ae0629ae07a6ae07a9ae07c0ae07c3ae07f4ae07f7ae080fae0859ae0889ae08f1ae093dae095aae0966ae09b0ae0a24ae0a3dae0a54ae0aa6ae0ac0ae0b36ae0b79ae0b9dae0c02ae0c4aae0c81ae0c82ae0cccae0cdeae0ce8ae0d08ae0d1eae0d24ae0d53ae0d98ae0e0dae0e28ae0e3fae0e67ae0e93ae0e97ae0ea9ae10e6ae10fcae1106ae1116ae1141ae1152ae1159ae116aae1198ae1389ae1393ae13b2ae13ffae1462ae146fae14c0ae14cbae1520ae1710ae172aae1739ae17acae17c0ae17fbae1924ae1975ae19b9ae19ddae1b91ae1beeae1c15ae1c1fae1ccbae1d6bae1d8aae1dabae1e44ae1e47ae1e67ae1e68ae1e90ae1eaaae1ec2ae1ed1ae1f24ae1f42ae1f4cae1fffae2005ae2014ae2018ae2038ae2039ae20c6ae20caae211eae214aae21cdae2670ae2695ae2696ae26b8ae26c7ae27a6ae27f9ae27fcae2ee9ae2eeeae2f56ae3406ae4793ae4798ae4799ae47b8ae47e2ae4887ae4888ae49c7ae49f7ae4a1aae4a60ae4b7eae4ba2ae4ba3ae4baaae4be0ae4be8ae4cf4ae4efeae4f13ae4f3fae4f64ae4f6aae4fb7ae4ff0ae50b8ae50ebae515dae5198ae51b3ae51d1ae51deae5209ae5243ae5245ae52b6ae52c5ae535dae54b4ae564fae568bae56b5ae579dae57d9ae5879ae589cae598dae599fae59daae59dbae59e7ae5a1bae5a4eae5a60ae5a65ae5a7cae5a8fae5ab5ae5ac5ae5ac9ae5ad5ae5b35ae5b6eae5c64ae5cb8ae5d3cae5d49ae5d4aae5d5aae5d6eae5d77ae5dedae5dfbae5e59ae5e87ae5ea1ae5ea2ae5ed1ae5ee4ae6024ae6027ae6072ae60f7ae618eae61fdae6211ae623bae6251ae6341ae6373ae6389ae649eae64b3ae660aae6953ae695cae6a4fae6a95ae6aa5ae6b7cae6c1cae6c1dae6c57ae6cbfae6d4dae6d50ae6d78c2b0c1c2bcfbc2bde1c87f04e48c8ae49182e49378e494a7e847d0e847fae84a270100820200b80200fc02ba6406428b0642fb08af3f0a40190a40240a40540a40580a40610ac0e70ac3ee0ac7e30ac7e60ac88f0ac9060ac9370aca0a0b427f0d05e70d082d0d09430d0bf1149c77151cf6152b041533fd1533ff154065154db21d334432002a32003e33fcab33fcae33fd0a33fd3e33fd6e33fd8833fd9233fd9833fda833fe7d33ffd033ffd233ffd93571ce3aaaae3aaac33aab413aab433aab853aab8d3aabcc3aabcf3aabd23aabde3aabe03b75393b763c3b76563b77b93e8b353e8b413f52db3f751b3f85803f85d2400ee343c1b643c1c643c2af43c32043c39b43c49443c4ad43c60a43c60f43c6bd43c6c043c6d043c72d43c79f43c7a843c7aa43c7e143c87c43c8c543c8c8447d3c44f12244f12744f64444f67244f67944f829451e92456789457c2346f483477fcf47811447812147812548040748040d48041e48047348084248088148104948d84448d895497c49497cb14984304984a44984c74a34d64a34e24a35e34a82f04a82f74aecbc4b7f6f4b7fdc4b80154b822b4b82a44b82d44b82e84b83444c2c3a501f8f502fa5505f6c505f72506e6d506f5f507f8e50807250ffc07102597102a87102e371030871030d71038071f90871fa01738a4a738b4975023b75867476103f76e72e781d697a427e7a428c7a43ea7a44507b006c7b09497cf8487cf8747cf87d7cf8d07cf9a97cf9df7cf9ef7cfaa48000d68002158002398002488002b08002d680031d80032f80033c8004fd800791800e2380151187cc0887cc1088140a883801896c13896c26896c3b896c498a09808a2901901010a1af83adfcd3adfc75adfcd7adfce8adfcf2adfcfaadfdc8adfe5eadfe89adff00adff43adff88adffb3adffcdae0004ae0010ae004cae0068ae0073ae0083ae017aae01ccae01d0ae01e7ae01e9ae01feae023dae0250ae02d1ae02edae035eae0363ae040aae047fae0486ae04f3ae04f7ae051aae0577ae0589ae066eae06e4ae07edae07ffae0803ae0868ae08baae0964ae0995ae09eeae0a0fae0a39ae0a47ae0a48ae0a88ae0b69ae0b6dae0b72ae0bfbae0bfcae0c11ae0c1fae0c40ae0c62ae0c74ae0d44ae0e3dae0e66ae0ebdae0f03ae10bcae10e9ae112cae1132ae1135ae117aae11baae11c6ae11e0ae1217ae1283ae12aeae135fae13b9ae13eeae1408ae143fae1467ae151aae151fae1733ae1743ae1790ae17f7ae1aaeae1bfcae1da9ae1db8ae1e4fae1e55ae1e8aae1eb3ae1f23ae1f32ae1f5aae1f61ae1f84ae1fa5ae1fc6ae1fc9ae202cae202dae20e4ae20f5ae215bae21c1ae223fae23bdae2606ae2708ae270aae272cae2760ae2762ae2863ae2f67ae3408ae4788ae47dbae4a0aae4a25ae4a7cae4b4bae4b85ae4bb0ae4bddae4cfeae4d2cae4ebdae4ec1ae4eebae4f5fae4f69ae4f76ae4fabae4fd9ae4fdcae4fe1ae4ff8ae50c3ae50d4ae510cae5110ae511bae517eae51b5ae51dfae525fae52dfae5313ae5641ae564bae5663ae5667ae5668ae566fae567bae5699ae56acae56b3ae574fae5788ae57c8ae57cdae5882ae58a2ae58a8ae593fae59dcae59f0ae5a7fae5b14ae5bc8ae5c1fae5c6bae5c9dae5ca0ae5cc4ae5d3dae5d4cae5d52ae5de5ae5e4aae5e5cae5ed4ae5ed7ae5ed8ae5ef3ae5f16ae5f18ae5f41ae5f96ae5f9dae5f9eae603bae6052ae6207ae6217ae622eae634cae6352ae6374ae638bae63b8ae6478ae648dae6490ae6494ae64a9ae65faae660eae690cae6914ae6998ae6a98ae6aadae6b96ae6baaae6bd3ae6bf7ae6c06ae6c33ae6c53ae6c60ae6c64ae6cc7ae6ce8ae6dc1ae73dcae7485ae7489ae74adae74b1ae77a0af04fcc2b085c2bcc9c87f23e4008ce400e9e483afe49413e4992fe49936e806420101a102003506a26406a28d06a28fc87f3908af3d0a40140a40390ac0ea0ac3170ac33f0ac3f50ac7da0ac8210ac92b0ac93d0ac9ed0acaf20ba0380c21430d05460d07dc0d08a7142b1d142e9b142f53142f6114f127150095151ced151cf2152b00152b03154069154e52156ee61f704032001f32002132003033fcc133fccc33fd2533fd2c33fd5533fd6233fd9333fdc333fdfc34308b3435cd34738d3474d43571c93571d13571d239a84039a8483aaaba3aaaca3aaacc3aab1d3aab203aab353aab4e3aab4f3aab663aab773aabb63aabb73aabe33aac1e3aac223aac2c3b76353b764a3b764d3b7b673b7b7c3b9bd03b9bf43e88193eb6b33f4f0f3f60053f6e703f8249400ea8400ee5447d1743c0b443c25443c2b343c31143c44043c4bf43c4dc43c5e243c69143c69743c6c643c6cc43c6f643c76643c76743c76a43c7a143c7d243c85343c8d843c8de43c8f843c91d43c92743c94e447d5144f04144f04244f16644f60244f61a44f806457c0e45f43846789b4681524682a648048348048a48048c48084548d88e48da604984ad4a36154a82fc4a8312506f65507f8c50ff3c60180068306a70626370c08070c0907102827102dd7102fe71034d7103ae71f9c471fe507433957586237586c97600e976e30576e30a76e7237a42527a42537a427f7a429e7bd03d7cf88a7cf9247cf9907cf99e7cf9d17cf9db7cf9e07cf9e57cf9ed7cfaaa800079800221800274800795800e8a87c40a87c82e883ac18953fd896c678a06e68a0a01ae26bbae277a0b4151adfca8adfcafadfdbeadfe1badfe4aadfe76adfea1adfed5adff77adff8aadff90adffa1adffd3adffd7adffe1adffffae000fae0055ae0072ae00b7ae00b9ae00baae00bdae00d1ae00e3ae0102ae013fae01fbae0230ae0243ae02cbae02d6ae0362ae0370ae0372ae03d6ae03ecae0413ae0419ae041fae0423ae0489ae049eae04a7ae04eaae04eeae055bae0582ae0597ae05a9ae0665ae068dae06d8ae07deae088aae0899ae08a3ae08aaae08aeae09deae0a31ae0a75ae0a79ae0a80ae0a85ae0ab9ae0b03ae0b84ae0b89ae0ba2ae0c00ae0c2fae0c67ae0cbfae0cf1ae0d00ae0d88ae0dc3ae0dc5ae0e03ae0e12ae0e17ae0e50ae10a7ae10b9ae10f6ae1113ae1121ae1150ae11b0ae11c8ae11d7ae123cae12aaae13b0ae13c5ae13c7ae1465ae1466ae1480ae14fcae1524ae1530ae1727ae1752ae178eae17e9ae183dae1859ae1897ae18bcae18daae19aaae1bffae1c1eae1c31ae1c35ae1d89ae1dbfae1e79ae1eafae1f3aae1f43ae1f50ae1f55ae1f94ae1fcbae201bae20bbae2107ae2141ae2148ae21beae24ecae2663ae268bae268dae269dae26b9ae2914ae2938ae2bebae2beeae2ee3ae2fa0ae34bfae47f7ae481fae49caae4a26ae4b66ae4b76ae4b94ae4b9bae4ba0ae4bf2ae4caeae4d37ae4e03ae4e15ae4ea9ae4eb8ae4ec5ae4edcae4fa3ae4fbfae4fc2ae4fcfae4fe2ae50b5ae50bdae50cfae50d0ae519bae51adae51b8ae51e9ae51edae5244ae5248ae5273ae52a9ae52b5ae52c4ae52e1ae52e8ae5300ae5317ae531fae54cbae54e9ae54efae54f1ae54f6ae5639ae566dae56a7ae56dfae5737ae574dae5787ae59a3ae59c3ae59c6ae59e8ae59efae59fcae5a18ae5a31ae5a37ae5a4bae5a53ae5a9dae5ad0ae5ae4ae5aebae5b38ae5b42ae5b70ae5b84ae5baaae5c68ae5c71ae5ca3ae5cdaae5cedae5d0cae5d30ae5d36ae5d3eae5d3fae5d76ae5d93ae5dbfae5e76ae5ec8ae5ef1ae5f95ae5fa3ae604dae61deae6234ae6255ae627dae63f8ae647cae649bae64a7ae64baae68b7ae6963ae698cae6a31ae6a34ae6a39ae6a8aae6a97ae6ac0ae6ba9ae6bd1ae6befae6c54ae6c72ae6cbeae6cdeae6d75ae6da5ae6e7aae73e3ae73eeae7473ae747eae74b5ae74c5ae77c5ae7811af38bec2afd1c2b1d9c87f0ee400a3e4012de4015be4955ce4993ce80675e8cfbae8cfc601018b0101920101980200630200b00200bc0200f002010902b261034443038f4304403f04c03f06a20906a21f06a25109012c0ac3c50ac8f30ba03914b3b714fbf81501c6152890152b091533e215535d32000232002632004432005533fca433fcee33fd0c33fd2233fd5133fdd633fdfe33fe2b33ff143571c13571c33571d03591813aaacf3aab023aab253aab2b3aab363aab883aabbb3aabd33b762d3b77753b7b863b7f6d3b9bb63b9bde3b9be03f43683f476b3f717c3fa50c3fa9333fa93b400eec43c22643c2a643c46143c4a543c4bb43c4c843c56343c5ec43c5f443c69f43c6c743c6d843c6f943c70d43c76443c7e543c7e843c8c143c91a43c92044f10744f16444f60344f67844f67f45f42245f4404678a3468111473c0c477f8e477f9e4781294804044804194804b64804b948080a480c2a480c2c48104de4992348d82348d82d48d8af48d91048da4c497c02497c26497ca6497cb649843349848b4a35e14a35e24a81144b7f7b4b7fa54b7fad4b7fb24b7fc94b7fcc4b7fd64b82994b82d34c2c3b4d03c6506f22506f44506f5d506f6950ff3d50ff3f683000683248702c0470428071012c71025d71039871fa14728132738a4c738a55738a9275050975862a7a42487a424e7a42a37a44117a44457a49437a496f7bc0127bd0327be0367cf8457cf84c7cf8557cf8577cf8cc7cf9d97cf9ea7cfa478002ad8002e780078e800e1f800f328014768016e887cc1987cd05881403881bf48835d2884017896c05896c47896c58896c66a11cf8ae5af0adfca4adfcaaadfcb7adfcc4adfce1adfd0dadfdbfadfe51adfe97adfeabadfebdadff09adff0cadff52adff5aadff83adff8dadffd9ae0030ae0038ae004fae0052ae008bae0093ae00b1ae00c6ae00d4ae00eeae00ffae0104ae0116ae016dae01caae01edae0221ae0222ae022eae0240ae02c8ae02ecae02f5ae0308ae035dae0385ae0386ae038dae0420ae042bae045eae0482ae04beae04dbae04e8ae04f6ae055fae05a8ae05e3ae0657ae07e1ae0823ae086fae0880ae089dae08f9ae0968ae099eae09a0ae09baae2698ae09dbae0a0cae0a3aae0a3bae0afdae0b70ae0b75ae0c29ae0c4cae0c6aae0c77ae0d28ae0d91ae0dbdae0dd3ae0e16ae0e53ae0e59ae0e5eae0f00ae10bdae10ecae1105ae110eae110fae111cae114dae1151ae1164ae119bae12a5ae12a6ae12c5ae130fae13acae13c3ae13f0ae1407ae1418ae142bae1437ae143eae14b3ae14baae14bbae1568ae1734ae1798ae1802ae1828ae184aae18c7ae18d9ae18e6ae18e9ae18eeae18faae1900ae1af9ae1c0eae1d61ae1dd0ae1dd3ae1e6aae1ecdae1f3fae1f49ae1f73ae1f81ae1f83ae1f97ae1fb1ae1fc5ae1fecae2010ae2016ae2036ae203cae2041ae2049ae204eae205cae212fae2132ae2146ae21c9ae220bae223dae23feae2651ae267cae26b1ae26bcae2918ae2bffae2ed6ae2ee6ae2efcae2f61ae2fa2ae2fb0ae2fd3ae478aae47a9ae47d8ae47f4ae4826ae482bae4835ae4859ae4880ae488cae49f0ae4a24ae4a51ae4ae5ae4b54ae4b75ae4b89ae4bf1ae4c04ae4d38ae4d66ae4d6aae4e12ae4e1cae4e9cae4ee1ae4f2fae4f4fae4ffdae50bfae50d1ae50d3ae5137ae5199ae51bdae51c2ae51d5ae51e6ae52bbae52d4ae52d6ae5302ae535fae5375ae54caae54e8ae54edae5643ae56c9ae5738ae5766ae577eae578cae57b4ae5872ae588aae58f1ae5966ae598eae59a6ae59cbae59dfae59ecae59f7ae5a29ae5a6bae5a7bae5abbae5ae5ae5bd4ae5bdaae5ccbae5d27ae5d2fae5d43ae5d63ae5d8bae5dbcae5dc6ae5e69ae5e90ae5ed3ae5f9aae6015ae601eae60c9ae60eaae60fcae6195ae61ccae61dbae621bae6257ae6284ae63beae63dcae63dfae63f0ae6496ae64a0ae64b8ae64c2ae6600ae660dae6966ae6990ae6999ae6a2cae6a32ae6a38ae6aa3ae6aaaae6aaeae6b8cae6b90ae6ba5ae6bafae6bf5ae6c10ae6c19ae6c25ae6cadae6cb6ae6cc2ae6d7fae73d7ae73daae74bfae74d8ae74e8ae77baae77c0aeaaafaed8b5aed9b7af3c5fc2c04dc87f08c87f22e20049e400c3e400cde47dfee48444e48750e48f43e48ff4e494a2e49932e49945e80674e8cfc7e8cfcbe8cfdb01008b0101640200c504c1a70940150a40150a404d0ac7ab0ac7d00ac7e10ac88c0ac9410acaf80c215b0d08ab0d0e1c33fd4c14247014247114343314345a151cf91526c8152bda1533c5154c3132001932002e33fca333fce733fd7033fd7733ff513431133543c33571cb35919439a8473aaab93aaace3aaaf13aab0e3b762f3b779c3b7b3b3b7b5c3b9bac3e826f3f4e273f5aac3f61aa3f6c263f80063f86ad3f88443f8d2e3fa1be3fbefc505f7743c07d43c20843c21943c39743c39a43c40a43c44443c44b43c45b43c47e43c60343c61843c67043c6bc43c75e43c78343c7cf43c8e243c92c447d05447d1f447d2c44f18444f68244f6834682d946f48446f592473c054781174804114804824804b348084948084e480c04480c4348d84248d89148da62497ca2497cbd4984934984964b7f654b7fa04b8209507f8350ffcb613133702c2570626770c08b70c08c71012d71025c71030271030671039c71f90171f90c71fa0271fa0871fc047386c07500d475837b76e30d76e7287a42847a42857a428d7a44037a49427a49de7cf87b7cf8987cf89d7cf8ec7cf9227cf9dd7cf9e67cfa087cfa8780026f8002758002be80079380079780171587c82b87cc1f87cc32881b72881ba58967da896c1f8a097f8a098eae0c42a3ad35ae08b2ae0c52ae0d0aae0d10adfc7badfc8dadfcacadfccbadfcceadfd11adfd7eadfd86adfdd4adfe4cadfe6dadfe74adfe7fadfe83adfea5adfecdadff50adff54adff5fadff66adff9dadffa6adffaaadffadae0080ae010dae015eae015fae0160ae0191ae01deae01ebae0236ae0265ae0269ae02d0ae02dcae035fae03f4ae0401ae0460ae048dae049aae04a8ae04c9ae04e1ae04fcae04ffae052cae05b8ae0627ae06ddae07e0ae0800ae0804ae0863ae08a4ae08b6ae08ebae094eae0997ae0a11ae0a6bae0b07ae0b0aae0becae0bf9ae0c2cae0c72ae0ccaae0cd5ae0cf2ae0cf4ae0cfdae0d29ae0da5ae0dc0ae0ddfae0df5ae0e92ae0e9dae0eb7ae0edbae0ee6ae10b3ae10baae10f7ae10fbae1118ae1128ae116fae1177ae117bae11adae120aae120bae1242ae1255ae128fae1290ae13baae13f5ae140dae1413ae143aae144dae144fae1458ae146eae1723ae1814ae182aae18f7ae1908ae1946ae19c2ae19e7ae19f7ae1b70ae1c06ae1c1bae1d52ae1d5dae1dc9ae1de2ae1e62ae1e63ae1e65ae1ebeae1fa4ae1fa6ae1fd6ae1fe6ae1fe9ae1ffbae2003ae2055ae2064ae2067ae20c0ae20c8ae20e3ae212bae215cae215eae21c8ae21d8ae23a0ae24c7ae268cae269aae26aaae26aeae2767ae2783ae2906ae2913ae2917ae29fcae2bf1ae2edcae2edeae2f63ae2f66ae2f6fae3403ae45c3ae478fae47efae483dae4878ae488bae4a09ae4a0cae4a18ae4b5aae4be5ae4cecae4cfaae4cfdae4e8dae4eb5ae4ed4ae4f78ae4f8bae4fc9ae4fceae4feaae4feeae50baae50d8ae5113ae5115ae51beae526aae526fae527bae5283ae5293ae5296ae52e0ae52eaae52efae52f6ae5378ae538dae540dae54d0ae54d2ae54d7ae54deae5644ae565fae568fae5696ae56b7ae56bcae56dbae56f2ae5729ae5735ae5751ae577aae57bcae58b0ae58efae5960ae596dae5970ae59a7ae59acae59d6ae5a20ae5a21ae5a6eae5a72ae5a7aae5ae2ae5b1fae5bcdae5bddae5c20ae5c7bae5c91ae5cd6ae5d10ae5d1dae5d2eae5d68ae5d6fae5d70ae5d73ae5e0fae5ea9ae5ebbae5edaae5eeaae5eebae5efcae5f3aae5f42ae5f98ae6011ae6037ae61b7ae61daae61e0ae6232ae627aae6340ae6358ae637bae6382ae6395ae63dbae6489ae6495ae64abae64b5ae690eae695bae6a70ae6a77ae6b95ae6b98ae6ba4ae6be8ae6c47ae6c4aae6c76ae6cddae6d3cae6d4aae6d5bae6dadae6e7fae7477ae749eae74b3ae74daae74dfae779aae77a4c2b049c87f29e2005de40050e400e5e40139e40489e483ade4993ae806a6e8cfc200b89b0101ae0101f40180a7038f4e06405106a21306a25406a27806a29106a29c06a39207007a0700dd0900f90940110a40100a402a0a403b0a40530ac7aa0ac8930acadc0d06bc142bfc142c17142ca114b57a14f1201501c71533d61533ef15535b32002932004032005333fca533fcc333fd2033fd2733fd2f33fd4533fd5633fd5d33fd6f33fdd433fe0333fe3733fe7b33fec633ff9933ffb433ffbb33ffbf33ffc233ffcd33ffce3415893421993535413aaaaf3aaae13aaae83aab1c3aab423aaba13b76343b7bf63b9b2c3b9bdc3b9bff3eb1a63f64f643c07143c1e343c43543c4de43c69043c6e243c6f343c74143c78443c7d943c7e443c8d643c92343c93843c93b43c93e43cae3447d27447d33447d3d44c1e944f02544f1a644f63644f68444f6a144f82745f42d45f432468196473c02477f85480440480c2848d8a648d96148d98048da82497c2d497c30497ca7497cc049843749848249848a4984b28a08674a34d94a35c14a35cf4a82fd4a82ff4b7f704b7f774b82d04ca3324ca41a506f425082af7061f770621670626f70c07870c07a70c07c71025a71fa0e738a9176410476e72f7a42827a494f7cf8407cf8467cf85b7cf99f7cf9b77cf9f17cfa107cfa517cfa5780020580024d800337800e3287c40587cc0b87cc1a87cc1c87cc1e87cd0488040b88040f881b85882248896c10896c728a08ce8a08d18a100e901016ae12b9ae5a07adfc77adfc7eadfcadadfcbaadfcc6adfd09adfd75adfd80adfdb6adfdc3adfe52adfe5cadfec0adfecfadff5badff5eae002aae0058ae006aae008cae0105ae0117ae0136ae0140ae0178ae0189ae01aaae01aeae0226ae0248ae024dae0307ae03f6ae046bae04a2ae04e5ae04faae0580ae0593ae05dbae060aae0624ae0674ae068bae068fae07cdae07d0ae07e4ae085aae08bdae0967ae09c1ae09dcae09ebae0a0aae0a7dae0ab5ae0ae4ae0bc1ae0b28ae0b77ae0baeae0bb9ae0c2eae0c44ae0c5eae0c70ae0cc5ae0d1dae0d25ae0d32ae0d36ae0d66ae0deaae0df3ae0e06ae0e1aae0e1dae0e88ae0e9bae109aae1179ae6cc1ae1193ae11f0ae1209ae1238ae138eae13aeae13beae140bae1441ae14a0ae14b4ae14c9ae14d7ae1526ae161dae178bae1795ae17bbae17cbae1838ae1841ae1872ae196dae1becae1c17ae1c26ae1d7cae5a13ae1db3ae1e61ae1e7cae1e85ae1e8bae1e9bae1ea4ae1eb8ae1ebfae1ecbae1f41ae1f7cae200aae200dae2031ae203fae2050ae2108ae21c6ae2208ae24e8ae2667ae266aae2684ae2694ae26d9ae26e0ae2909ae29dcae2be0ae2bf8ae2bfbae2ef9ae2facae47f0ae4815ae4816ae4823ae485eae4879ae498dae49dcae49dfae4a2cae4afeae4b4aae4b4eae4b69ae4b8bae4be1ae4cf8ae4d98ae4e0cae4e13ae4e9aae4ea4ae4ec4ae4eceae4ed9ae4ee3ae4f99ae4f9eae4fa6ae4fc6ae4fc7ae4fe8ae50d5ae51c9ae51dcae51ddae526eae527fae52adae530cae5311ae531aae5361ae5370ae539eae54c7ae54f3ae565aae56b8ae56baae56c4ae571bae5777ae5784ae57c7ae5997ae59c1ae59f9ae59feae5a14ae5a3fae5a61ae5ac1ae5ad8ae5b86ae5c16ae5c6dae5c9aae5caaae5ce0ae5d47ae5d5bae5d72ae5d7eae5d8aae5eaaae5ec3ae5edcae5ee7ae5f46ae5f50ae5fafae6007ae6020ae60ffae61c1ae61ceae620fae6222ae6236ae632aae6347ae6388ae63ebae6408ae647eae6487ae648fae6497ae64a1ae64adae64b0ae68bfae68f0ae6910ae698eae6a23ae6a30ae6a71ae6a8dae6aafae6ba1ae6beeae6c3bae6cabae6d3dae6d7cae6d7eae6d80ae6daeae73d8ae73deae7448ae7484ae749bae74cfae74deae74e0ae77a3ae77ddafa827c2b053c87f2ce485e8e4891fe48c89e80647e84801e8480306a2560100750101b00101d006a24f06a2520741fa0a40250a40260ac3f40ac82a0c40540d836e14633d15279c1528851528921533f815405d1540761d33f533fc9e33fca733fdb633fe1033fe2333fe6f33fe7033fe7333ff1733ffc933ffda3474d53474d835350d3aaab03aab6c3aab9a3b76493b76883b76b43b7b3f3b7f723b9bb13b9bcd3b9bd83eac693f431a3f532c3f65333f727c3f881c3f89353f927c3fa82a3fa9353fab7643c06e43c07743c31443c48143c4b043c56843c61543c67443c68243c6a343c6e343c6ea43c74743c7a343c7e943c88543c8ce43c8d343c8d443c8e843c8f743c92243c92443c93243c94243cb0b447d09447d1944f10246788d46789d46815046830c47813048040848041048042048048e48048f48081148d82c48d88148d8e048d90c48d9cd497c054a34d74a35fa4a82f24a830a4b7f4c4b7f8b4b7fa64b7fcb4b822c4ca1ea4ca331505f68507f845082565110eb702c2070622e70625d70626b70c07b70c08f7103057103b071dd2371f904738b42738b437434c57434d4ae4f2076e30c76e73378cf957a428f7a44007a44167bb1537bd0697cf8367cf9cd7cfa157cfa7f80027b8002a08002ae8002c3800e1e800e26800e69800e79800e8c87c84187cc0287cc27881b63881c0c884005884008896c04896c17896c68ae4f65adfc6eadfc7fadfc92adfcf9adfcfcadfd72adfdb4adfdf9adfe1aadfe20adfe53adfe55adfe64adfe7aadfe7cadfe8dadfecaadff64adff72adff9fadffa7ae0016ae002fae0031ae0036ae0111ae0137ae0167ae01cdae01d5ae01f0ae01f7ae0272ae0394ae03faae0462ae050aae0572ae0598ae063fae0650ae0664ae067aae06dbae074fae079fae07d5ae0882ae0884ae09acae09bcae09f9ae0a1dae0a25ae0a30ae0a8cae0aafae0b73ae0b7aae0b99ae0ba0ae0bc4ae0bdbae0c18ae0c1dae0c3bae0c47ae0c60ae0c86ae0cb5ae0cf9ae0d16ae0d83ae0dd1ae0de3ae0e02ae0e13ae0e23ae0e4aae0e61ae0e6bae0e74ae0efeae10f4ae10faae1108ae1134ae11afae11b8ae11bbae11ceae11e1ae1292ae12a9ae13a8ae13fbae1414ae145dae1470ae14a4ae14c2ae14d5ae1730ae1747ae1755ae17a2ae17abae1805ae1856ae185dae18edae1972ae1bf1ae1c16ae1c25ae1d13ae1d78ae1dbeae1dd6ae1ddeae1e49ae1e56ae1ec6ae1f4dae1f6fae1f74ae1f89ae1f9eae1fbeae1fc0ae1fc8ae2001ae2021ae203eae2119ae2129ae2138ae220aae2238ae223cae2697ae26b7ae26edae26f7ae2741ae275eae2901ae2915ae2f72ae2f73ae2faeae478bae4790ae4791ae47e9ae47eaae481aae481cae4827ae482aae4843ae4870ae4992ae4a52ae4af9ae4b46ae4b71ae4bbaae4bffae4c5eae4d67ae4e95ae4ea1ae4ee4ae4f9fae4fbbae4fefae503dae50aeae510bae5116ae5181ae51baae51e5ae523cae5250ae528cae5298ae52ddae52e7ae52f5ae531eae5369ae537eae53a2ae5647ae5653ae5662ae567fae56a8ae56adae5724ae5739ae574aae577dae5793ae57a6ae57a8ae57b7ae57d1ae5877ae58acae58e3ae58e9ae58faae597cae59adae59b3ae5a11ae5a30ae5a48ae5a80ae5a89ae5a96ae5a9eae5ab9ae5ae8ae5b41ae5bc1ae5bc3ae5cb4ae5cfeae5d2aae5d64ae5d71ae5d99ae5dd6ae5de4ae5e10ae5e8aae5e92ae5ee6ae5f09ae5fa2ae606eae60d2ae60d6ae6101ae61b3ae61b4ae6200ae620bae6210ae621cae63baae6a3cae6aa1ae6abcae6ba0ae6ba2ae6bb2ae6bb9ae6bd2ae6cdbae6cf9ae6d5fae6da1ae73e7ae74b9ae74ecaedb4faef94baf8601af9ba5afa82bc2bd05e20004e4007be4010be80643e8064d010083038f4404c20c06a20e06a24706a25806a27a06a27c0ac88b0ac8dc0ac9fe0aca240acadd0d06150d07c70d09450d0a20142d8514f11614f124151cd7151cdd151cea1526eb15405a15520a32007333fcad33fd3233fd5233fda333fda433fdd233fe0033fe3633fe7533febc33ffb733ffd533ffe933fffb3425d63532c63563433aaaec3aab163aab193aab323aab3a3aab793aabb93aabd43aabe93aabf63aac123aac173b766b3b76893b77833b9bd23b9bf13b9bf83e80c03e865f3e8b133f65213fa9383fb0e63fb2b243c25143c2a143c33f43c4ab43c4d343c4d643c54b43c56143c5e343c6b943c6d543c73843c77043c77d43c78243c79743c7af43c8b543c8f343c90b43cae848104a447d07447d28447d2e44f0e244f42944f42e44f65344f66344f66a457c03457c1845f42e46788e46788f4678a74682e548049548080948086048086c8016ee48c45e48d960497c04497c28497c2e497c2f497c35497c3e497c3f497c84497cab497fa24984434a35a24a35c84a81994a83014aecbf4b7f5e4b7f764b7f9b4b7fb34b7ff94d03cd501fbb505f6a505f6b6831f2702c49702c8770626270626a70c09671037e71039771f9c1730903738a5a738a5d758628758659762bf4766048ae6c1a7a44187a443f7a48937a491a7a49347bb1697be0487cf8527cf87a7cf8ed7cf98f7cf9b07cf9c87cf9da7cf9e77cfa0f7cfa147cfaab8002918002988002cb8003248003e58003e687c81e87c82887c82987cc22881b6d881ba48967db896c06896c1a896c57896c5f8a07078a09e2ae6c5dae0468ae2793ae04acadfca6adfccdadfcdbadfd12adfdf8adfe81adff05adffa3adffa4adffa5adffdcae0000ae0046ae00d6ae0115ae013aae0162ae01efae025cae02daae0368ae0436ae0457ae045aae045dae04d7ae04f4ae0505ae0570ae057dae059bae0681ae06dfae07aeae07b4ae07ddae0818ae0885ae089fae08deae08e8ae0996ae09aaae09c5ae0a04ae0a05ae0a13ae0a46ae0aa0ae0ac7ae0b65ae0bfeae0c3fae0c45ae0c58ae0c92ae0c9cae0cedae0d1fae0dcbae0dcdae0de0ae0dfbae0e56ae0e5fae0e6cae0e94ae0e95ae0ea6ae10f3ae10f5ae10f8ae1100ae113cae119aae119dae11f8ae1234ae127cae1346ae13a2ae13a6ae13bcae140cae1449ae1452ae1453ae14bfae16f1ae170cae179aae179bae189aae18c1ae18f1ae18fbae1921ae19fbae1bf0ae1d8cae1d97ae1e45ae1e50ae1eaeae1f1bae1f26ae1f2fae1f47ae1f79ae1f7aae1f9cae1fbaae1fc2ae1ff3ae2015ae206dae6bc9ae20c9ae2136ae21e8ae23e3ae264fae26a7ae26a9ae2796ae27faae2beaae2c39ae47aaae47b3ae47bcae483eae485fae4860ae4872ae4883ae4a0eae4a4eae4b27ae4b41ae4b60ae4b9dae4cfbae4d27ae4d63ae4e8eae4ea7ae4ebeae4eddae4fdbae5118ae516fae5262ae526cae52dbae5371ae5382ae538eae53caae5645ae568eae56cfae56d5ae5775ae5785ae57a0ae58f6ae596fae59a2ae59f2ae5a15ae5a66ae5a78ae5a79ae5acdae5ad6ae5aedae5b17ae5b75ae5be0ae5c90ae5cb3ae5ce5ae5d5eae5dcaae5deaae5e51ae5e9eae5edeae5f0cae5f11ae6008ae601bae6034ae60ebae6191ae61c9ae633cae634eae637aae638dae6392ae63f4ae641bae6477ae6483ae6485ae6488ae64a4ae64a6ae64bfae6911ae69c5ae6a1dae6ab8ae6c66ae6c7aae6c83ae6cfdae6d59ae6d79ae6d82ae6dbfae6ed5ae7402ae7488ae74bcae74edaf5149af61caaf8458af847ac2b0ade47f5be49181e494a5e49947e80673e847ff0101b10201b206a20a06a27506a28c06a2970a402d0a403a0ac89d0ac9280acb5f0ba0040d07cf0d088c0d09460d0a3114f11d1501771501cd15288a152bc41533d91533fc1f333f31ff3c32004832005233fcb033fcb333fd3a33fd3d33fd6a33fd9c33fdd033ff1633ff1933ffa133ffbe33ffeb34308734308834738b34738c3594053aaaaa3aaaab3aaab23aaae23aaae33aab3e3aac253b75c83b75cc3b75cf3b75e03b763e3b76413b77783b7b743b7b763b9bdb3b9be13e801e3e81823e8a4a3e8c6b3eade845f44d3f4b1f3f6cc93f81e33f984f3f9ae83fa9403fb96c400eea43c13643c15543c1e543c26a43c2a343c32343c48743c4cc43c51e43c55143c55a43c5fc43c61d43c6d643c70c43c79643c7cd43c88943c8be43c8ca43c8cd43c8e343c8f443c92a43c92b43c92f43c963447d2f447d6844f16844f64344f65044f66b44f67444f68646789346789446789a468100477f9d477ff547811647812b47812c48041348042348044848d84148d84348d85348d903497c99ae5f174a35a84a35d04a36104a81854a82f94b7f614b7f954b7f974b82064b820d4b82104b82c64ca1584ca3364ca41b4ca41e4d03c84d03c9702c3270626970626e7102617103b50ac9ea71d87071f90671f9c371faf371fc017434c17585d57585d775862276e31476e72176e73079193a7a43ff7a44027bc0377cf87f7cf89e7cf8c87cf8d97cf8db7cf9a87cf9d57cf9f48002aa800e2187c81a87c837881402881404881b6f881b848836d3883b84884007884b038853328967d1896c1d8990018a01318a2908901011a6fa0ba9a198adfc9badfcabadfccaadfcdfadfce0adfd8aadfe0fadfeceadfed1adfedcadfef1adff75adffacadffefae0001ae003aae0045ae005fae0065ae007fae008fae00edae00f3ae0139ae01c8ae0212ae0227ae0242ae02d2ae02dfae02e0ae0313ae0364ae0366ae0375ae0384ae03f7ae03f8ae0403ae0409ae0441ae04ecae055dae058aae05abae05adae05deae0614ae0661ae0685ae07b3ae07daae07efae088dae088fae0893ae0897ae093fae0945ae094cae09b1ae09bfae09c8ae09fcae0a15ae0a1aae0a3fae0a41ae0a83ae0b00ae0b40ae0b68ae0b83ae0bbaae0c4bae0ca4ae0cd4ae0cebae0d4fae0dbbae0dd4ae0dedae0e25ae0e44ae0ea8ae0f04ae115dae11c9ae11e9ae12b7ae13b7ae13cbae13ceae1404ae144eae145aae147eae14d0ae14d4ae15efae1689ae178cae17b9ae17efae1849ae186dae1898ae18f8ae1c34ae1daaae1e46ae1e59ae1e5bae1e74ae1ea8ae1f1dae1f33ae1f3bae1f86ae1f8fae1fa7ae1fabae2047ae2053ae205eae205fae2066ae2071ae20bfae20c7ae2130ae214cae215dae216eae21efae2230ae266dae26e2ae2794ae2797ae2907ae290aae291aae2c3aae2f23ae2f9eae4796ae479cae47e8ae4834ae4867ae4871ae487eae4885ae49c6ae49ecae49fbae4a0fae4a15ae4a20ae4b2dae4cadae4cefae4d3eae4e94ae4ebfae4ed0ae4ed2ae4ed7ae4edaae4f68ae4f8aae4f95ae4fd3ae4fd6ae51a7ae523eae52e6ae5367ae537aae5392ae543eae54eeae5684ae56a1ae56b2ae56c0ae56f0ae578eae57c2ae57d4ae5964ae596cae5995ae59e0ae59e9ae59f4ae59fdae5a0fae5a28ae5ab2ae5ac7ae5adfae5b0fae5b3fae5b49ae5b6fae5bf5ae5c67ae5ca8ae5cafae5cbbae5ceaae5d0bae5d13ae5d18ae5d4dae5d74ae5d94ae5dc2ae5df7ae5e12ae5e57ae5e9aae5eafae5ee3ae5eedae5f37ae601cae6021ae6039ae6041ae6106ae618cae6209ae623aae624cae625bae625cae636dae637eae63e6ae63f1ae63f6ae6404ae6405ae649aae64b6ae6997ae6aabae6b9bae6c15ae6c16ae6c44ae6c63ae6c84ae6ca7ae6cbaae6cc4ae6e75ae6e77ae73ddae7479ae74b2ae74e3ae77d3ae77feaedb53af3c88c87f0bc87f38c87f3fe14c32e200c7e4007de4010ce40110e41aa3e49258e49593e4992601007e0200330200380200c706428a06a21806a21906a27e0a404f0ac0710ac0e60ac8970ac9300acaf60d05680d07c50d0e6b142c1a142c9414fc0315287e152afa1533ee15508215537c32001532004a32005e33fcaa33fcdb33fd2833fd4033fd7533fdb833fddf33fdeb33fe6c33ffee34151734738e3532c935464b39a84f3aaabf3aaac43aaacd3aaad83aaae93aab123aab133aab173aab373aab493aab6f3aab743aac1c3b76393b7b3d3b7b753b9bfa43c43643c49e3f551d3f60073f85693f89b03f8a643f9c843fa9963fb3163fb34d400ee143c67c43c4a243c67f43c09843c0db43c19843c21843c29043c2a943c31943c56543c5e143c5ed43c6d743c70643c73e43c8ef43c93543c96743caed447d1644b2ad44f10444f10644f14144f18944f1a344f66e44f67344f68744f68844f6a344f8464678a646807946807a46815146815b477f9248040c48041648041d48044648050848050e48080c480854480861480882480c22480c2448104b48d82548d8e2497c2c4984294984b64a36014a830b71f8814b7f6e4b7f814b7fb04b7fb94b7fdf4b82074b82084b82214b82d14ca1ec4d03c54d23b3503fd5505f69506f68507f8150813150ffcc702c2a70c0927102e271030c7103847103d574358b7500c57503f87504357586b976e30976e30e76e72c7836057a42187a427c7a427d7a42a27a44017a493d7a49d27b0fcd7bb1527bcaac7be0247be0457cf8327cf8517cf89b7cf9a67cf9b67cf9c47cf9d87cf9de7cf9f77cfa487cfa848002288002b58002fa8002fb87cc30881b96881bb1881bc8884016896c25896c4c8a05808a0846adfc6cadfc91adfcb3adfcdaadfdcbadfe5badfe68adfe80adfea0adfeb4adfed6adfed7adff45adff56adff86adffb5adffb8adffdbae000bae0027ae002bae007aae0092ae0095ae00d3ae00f6ae00fcae00fdae0197ae01c9ae01d9ae01e6ae02c7ae02f2ae02f3ae0383ae0387ae03d5ae03fbae041aae0426ae04a1ae04aeae04c4ae0500ae0564ae0573ae059fae05aaae05baae0607ae0611ae061cae0658ae065fae0668ae0673ae0752ae07afae07d7ae0810ae088eae08bcae08e3ae095cae0961ae0965ae09a1ae0a16ae0a28ae0a38ae0a44ae0aabae0ae6ae0b19ae0b7bae0b7eae0b80ae0bacae0bb4ae0be1ae0c0aae0c32ae0c36ae0c5aae0c64ae0cbbae0d31ae0d39ae0d3aae0d61ae0d80ae0e1bae0e42ae0e4eae0e7eae0eb4ae10b8ae10e7ae1149ae1155ae1160ae1196ae11e3ae11f2ae1205ae139bae13a0ae1401ae1406ae1443ae1450ae1477ae14b5ae14cdae14d6ae14f8ae1514ae15c9ae16f4ae16fbae1728ae179eae17a1ae17ffae184eae59a4ae18f3ae1948ae194eae1951ae19eeae19f6ae1b77ae1bf3ae1d3eae1e53ae1e70ae1e8dae1e93ae1eb2ae1f72ae1f92ae1fb8ae1fd2ae1ff0ae201933fd4eae2030ae203bae2046ae20deae2178ae21a1ae21f0ae223bae24f1ae2680ae26baae26f5ae276fae27a4ae27f7ae2900ae293aae2bf3ae2bf6ae2c00ae2ed5ae2ed9ae2fabae479bae4817ae483bae487fae4882ae4a19ae4a2aae4b35ae4b5bae4bb9ae4beeae4cb1ae4cf0ae4d56ae4ddeae4e17ae4e18ae4e97ae4ecdae4eeaae4fa8ae4fb0ae50beae50ddae51d3ae51e3ae5251ae5272ae528aae52b3ae5362ae536bae5657ae5676ae567eae5680ae56ceae5723ae5750ae577bae578dae579eae57a1ae5894ae589aae58a0ae58a6ae5940ae595eae596eae59b6ae59ccae59f1ae5a02ae5a03ae5a04ae5a75ae5a8bae5a8cae5a91ae5abeae5b26ae5b2cae5b2fae5b74ae5bb0ae5bbdae5c6eae5c98ae5ca7ae5ce1ae5d2bae5d31ae5d4bae5d86ae5de6ae5decae5e04ae5e8eae5e9dae5ea7ae5eaeae5ebdae5f03ae5f08ae5f1aae5f40ae5f85ae600eae602cae60f4ae61c2ae6215ae62f8ae6337ae6344ae6359ae6370ae63b1ae63ccae63f2ae63f5ae64aeae6601ae690aae6a16ae6a2bae6a2fae6b86ae6c41ae6c70ae6c88ae6cfaae6dbcae748eae77c6c2b0b7c2b39bc87f0dc87f26c87f2ac87f32e2002ae4009de400a1e49286e9406000eafd01007b0100870100890100d301012c0101ac02003b0200f3038ff006a24906a25506a299480c050a404c0ac33b0ac9430acbb20c20da0c40520d0ab0142e22142e46146849151cd415271515533732000a32001732002032003132003432004332005c32006333fd3733fe0b33fe0c33fe1833fe1b33ffb233ffcc3aaad63aaaf43aaaf93aab463aab6d3aaba23aabd93aabf33b76013b76253b76263b76523b9bfe3df5bf3ea1cc3eb86643c27b3f47fa3f62fc3f9a923fb1793fba0643c28b43c39443c0cc43c1be43c26543c4af43c68543c6b843c73d43c76343c7a043c87b43c8c743c8e043c8e543c93c43c94f44f0a644f16044f16944f66544f8014678a14682eb477fa148044a48080748085748d88748d892497c24497c32497c814984264984574984874aecba4b7f784b7f9d4b7fbd4b7feb4b82c54d03c1505f6f506f61506f66516204516205702c6a7061f570621770626671021d7102ae7102c471031171df0171f29b74358775012e7502a876172576410276e3067a42467a44197a48967a495d7a49ee7b005f7b0eab7b70277bc0107be02a7cf8307cf84e7cf8807cf8927cf99c7cf9a37cfa017cfa047cfa0d7cfaa57cfaa67cfaa980028f8002b88002c58002cd800796800e42801478896c5687c41387c41687c41a87c83187cc1b87cc4988140188140c884361896c09896c59896c648a01328a08bc901012ae0b7ca2172eae0bbdae0bbeae0c20ae606aadfc64adfc93adfc94adfc9fadfcbeadfcf1adfcfdadfdc2adfdcdadfe77adfe7eadfe84adfe87adfec9adfed3adffa9ae001dae0050ae005bae0077ae0085ae008aae009eae00c9ae00dbae00f9ae00faae0163ae0179ae01c2ae024fae02d5ae038cae03cfae03f2ae0415ae0425ae0456ae04cdae04dfae04e9ae0592ae059cae05a0ae05e1ae060eae061bae0620ae066bae0694ae07f1ae0805ae0816ae0888ae089bae08ecae094dae09a2ae09d1ae0a29ae0a7bae0a87ae0a9eae0abfae0ae2ae0af9ae0b09ae0b30ae0b32ae0b3fae0c53ae0c6dae0cb9ae0dacae0db6ae0dbaae0e04ae0e0cae0e0eae0e1fae0e24ae0e2bae0e36ae0e5bae0e5cae0e68ae0e6aae0e96ae0e98ae0e9aae0ed0ae0edeae110cae113bae114cae115aae116cae1174ae1175ae11b1ae11c2ae11ebae11f6ae123eae1245ae1294ae13a4ae13adae13f9ae1400ae140aae1415ae1417ae141dae1479ae1482ae14b8ae16f5ae1700ae1702ae171fae174cae17a8ae17b1ae1852ae1866ae189cae18aeae1b86ae1c0cae1d7bae1db5ae1e7bae1e81ae1e83ae1e88ae1ebcae1ecfae1f87ae1fb9ae1fd1ae1fddae1fe0ae204cae2058ae2134ae2142ae2151ae220dae2660ae269cae26f1ae2774ae27fdae29d5ae2ef4ae2f13ae2f5fae2f6cae2f71ae2f97ae2f98ae2fa6ae2fafae35ccae479aae47acae47adae47b9ae47bdae47e6ae47ffae4820ae4821ae4849ae487aae488aae498bae49daae49deae4a1cae4a7aae4ae2ae4af1ae4af8ae4afcae4b31ae4b49ae4bb8ae4cf7ae4d60ae4d9bae4e10ae4e90ae4ee2ae4ee8ae4f29ae4f7bae4f88ae50e8ae51c4ae51cfae51d9ae51e4ae5247ae524eae525bae52daae536dae53feae54cfae54eaae5646ae5672ae56e9ae5720ae5768ae579aae57bdae57c1ae57c3ae57ceae5875ae58aeae58b8ae58d3ae58ebae595dae59d5ae59eeae5a70ae5ae9ae5b36ae5b8cae5bd0ae5c55ae5c63ae5d5dae5d66ae5d88ae5d91ae5d96ae5dbdae5e0bae5e6fae5e84ae5ea0ae5ea8ae5edbae5eddae5ee2ae5f3eae5f47ae5f93ae5f9bae600bae600cae603eae60ecae6197ae61bfae6221ae623eae624dae6383ae63b7ae63e0ae6402ae6407ae6471ae660cae690dae698aae6a75ae6a89ae6a8bae6a92ae6a9aae6abfae6bffae6c0bae6cccae6d3eae6d54ae6d8eae6da7ae6dbaae6dbeae6dc0ae6dc2ae6dcaae6dcdae7449ae746dae747fae7483ae748cae74c0ae74d6ae74e6ae77caae7801af3c49c2bfc1c87f10c87f2fc87f40e20018e40093e483d2e49287e4992be8065ee8068ee8cfd8e8cfda01007702003a02005f0200b502011a06a25c06a27d0900fc09cd450ab0420ac3f20ac9320ac93c0acb180d82130d8216142c4f142e12150011152b37152bb2154068154071154eb632001432004532005d32007433fc9233fcb233fce033fd3133fd4233fdd733fdf333ffdb34158b3426953571ca3f9b113aaab43aaac03aaad93aab153aab453aab4c3aab9e3aaba43aabb43aabc13aabc83aabe83b7b5e3b9bcf3ea12c3f81fa3fa9393fb4aa43c07c43c17543c17c43c1b343c1cc43c1ff43c29e43c2b543c31043c39943c4e043c5db43c5e943c5fb43c66f43c6e043c6e443c70043c70743c73a43c74943c75b43c78143c79843c8b443c95043c9b343caef43cf30447d21447d54447d5844ed8344f14744f67a4680784681764682bd477fcd478131480414480478480499480810480859480c2d48104748104f48d82448d82848d845497cc7497cdd4984484984974984a74a82fb4a830e4a83154b7f434b7fda4b7ff54b7ffe4b82c04b82db4d206a505f75506f4350ffee704f8171027d7102fa7102fb71df03738a03738b4a7500bd7502a57503f77a429d7a429f7a492e7bb15c7be0167cf83b7cf88b7cf8977cf9b57cf9ce7cf9d37cf9f37cf9f87cfa007cfa097cfa3280020480020e8002ab8002cc80033387c40187c41087c826880029881ba38967d3896c15896c24896c2e896c41896c63896c6c8a00028a02dc8a0328adfc73adfc7aadfc9cadfc9eadfce6adfcf8adfdb7adfe59adfe6cadfe7badfeaaadfed9adff4eadff76adff8fadffe5adffecae0006ae0008ae000dae0057ae0067ae0069ae0076ae00e4ae0143ae014fae01fcae0234ae0252ae0253ae0310ae0314ae0355ae036aae03eaae03fdae0484ae04e7ae0560ae057aae0595ae05a7ae065aae0750ae07a5ae07c6ae080dae0815ae0819ae094bae09c9ae0a0bae0a3eae0a74ae0a9cae0a9dae0afaae0afeae0b3dae0be3ae0c2aae0c7dae0cdbae0d38ae0d3bae0d64ae0dc9ae0e14ae0e3bae0e8cae10bfae10fdae1114ae1154ae11b5ae11c1ae11faae13b1ae13b4ae13e6ae13f4ae1410ae1456ae1459ae147fae14a2ae14ccae1532ae1724ae1731ae1738ae186aae18acae18ecae18f4ae194fae19c1ae1bebae1bf8ae1bfdae1dafae1db7ae1dc2ae1e96ae1e99ae1ed9ae1f39ae1f51ae1f56ae1f57ae1f78ae1fa3ae1fafae1fb3ae1fd4ae1ff8ae2043ae2057ae20bcae20c1ae2114ae213fae222eae2482ae24e4ae2650ae2674ae268eae26a3ae26acae26d8ae275bae290dae290fae29d4ae2be5ae2ed2ae2ed7ae2ef6ae47a4ae47b7ae47d9ae47dfae47e7ae47edae49c8ae4a13ae4a1fae4a7dae4b50ae4b74ae4bd9ae4bdfae4cb0ae4cf1ae4d2dae4d5bae4e93ae4ea0ae4ea6ae4eb2ae4f27ae4fadae4faeae5026ae509fae50b0ae50e9ae5112ae51a2ae5208ae5240ae5269ae528bae5292ae52ecae52f1ae530bae536cae5395ae5664ae566aae5673ae5679ae569bae56ccae57a7ae57d0ae595cae596bae5996ae59abae59d2ae5a23ae5a5aae5a64ae5aa1ae5ad4ae5ae6ae5aefae5b8dae5bb1ae5be1ae5be3ae5c54ae5c76ae5c8dae5c93ae5ce2ae5d3aae5d3bae5d7bae5d7dae5d80ae5d90ae5d97ae5dfcae5e00ae5e05ae5e9cae5eb7ae5f04ae5f4dae5fa5ae5fa9ae6010ae6036ae6047ae604fae61dfae620eae622bae623dae6298ae633fae634aae634fae6350ae63ceae63d9ae6403ae647bae648aae64b4ae68b9ae6987ae6a2eae6a78ae6a7fae6abeae6b94ae6ba6ae6ba8ae6c0eae6c48ae6c6eae6c75ae6ccbae6ccdae6cfbae6d4cae6dc3ae7470ae747aae7481ae74d5ae74d7ae74eaaf61c6c2bcf1c2b07bc2bd19c2bd23c87f00c87f06c87f2be14d73e20028e200efe40109e485e9e487d4e4916de494a63aab04e84919e88022e8cfc10101a302003e02004006a21606a25006a25d06a29407007b0ac77b0ac78e0ac8300ac93e0d06d90d08963aab1b0d821814fbc614fc071526be1526db152897152b0a1533de15340215405c15508532000e32004b32007c33fcc633fd0b33fd3433fd4333fd4933fd5033fd5733fd9933fdc133fe1a33fe2c33ffad3546413571cf3aaabd3aaac23aab283aab733aab823aab8e3aab933aab953aaba93aabb53aac133aac1d3b76403b77793b7b433b9bab3b9baf3ea5563f49e63f4dca3f89763f8b8e3fa9343fa93c43c11743c14843c16843c1e443c29543c29943c32b43c49643c4bc43c4cf43c4d043c4d543c4d743c54f43c5f943c67e43c69943c6d143c72e43c75743c76e43c79243c79e43c7e743c87943c87d43c8b343c8cc43c8e143c93644982544ed8144f10944f64244f6a044f82146789148043248044548080548b15e48d82e48d89848d8a548d90948d98149842e4984424984884984b14a35a54a36144a81824a81884b7f724b7fae4b7fb54b821f4b829a4b82a706a26b4ca41d503fd0503fdb506e1c507f826831f1683243702c3b70626571026f71030a06a27771f90b738a8f7503277610527a42037a42077a42237a42437a42837a44757a448f7a48957be04c7cf8677cf8797cf8f07cf98d7cf9f07cf9fb8002768002c480079c800e16800e20800e7080150d80151c80151e87c41587cc0087cc2187cc3187cd0387cd23881b68881bee881c02894087896028896c07896c2c896c3d896c40896c46896c7406a2988a0a00ae07c8a6fa32a7cae8ae6330ae636fadfc83adfc89adfc8aadfcaeadfcccadfdb5adfdbaadfdd1adfdecadfe63adfe82adfeaeadfebcadff55adff6fadff79adffdeadffe4ae0047ae005dae008dae01ceae01d7ae01e3ae0203ae0237ae0264ae02ddae0325ae0356ae0376ae03efae0407ae0487ae04b2ae04c1ae04deae055cae0662ae0663ae0679ae0689ae07abae07bdae63bdae07e9ae0883ae08b8ae09aeae09b4ae09d8ae09f5ae0a17ae0a43ae0abaae0affae0b01ae0b6bae0bc8ae0be8ae0c22ae0c7fae0c83ae0d23ae0d63ae0d6fae0da7ae0dcfae0decae0e0bae0e10ae0e32ae0e40ae0e55ae0e5dae0e73ae0eb6ae10ffae1109ae1138ae1139ae113dae1147ae1153ae1162ae12b6ae1396ae13f3ae1403ae1405ae1425ae149eae14a6ae14a9ae14c8ae1521ae173aae1758ae179fae17b8ae17f3ae183bae1873ae18e5ae18fdae197dae19bbae19e3ae1bf4ae1d00ae1d5bae1e4dae1e54ae1e5cae1e8fae1e98ae1ebbae1eceae1f31ae1f45ae1f4aae1f4fae1f65ae1f95ae1fb5ae1fdaae2008ae200bae2040ae206fae215fae2209ae2237ae23e1ae24e6ae24ebae267bae63c1ae26f3ae2709ae27f3ae27f8ae2becae2ee7ae2f19ae2fd7ae47beae47f6ae47fcae485bae49c5ae49e6ae4a4dae4a81ae4b3bae4b57ae4b58ae4caaae4e01ae4e11ae4e1fae4edeae4ef1ae4f36ae4f83ae4f96ae4f9cae4fa0ae4fc3ae4fdfae4fe0ae4febae513bae51b1ae5268ae528fae52a0ae52abae52f2ae5305ae5315ae5316ae5321ae5374ae5381ae55a0ae567cae5681ae5683ae56a0ae56d1ae56e4ae571aae5790ae5796ae57b8ae57ccae58b2ae598fae5998ae59f5ae5a57ae5a8aae5aadae5ab0ae5ac8ae5adbae5b45ae5b69ae5b89ae5bdeae5be6ae5be7ae5cb9ae5d37ae5d55ae5d57ae5d7aae5d7fae5d8dae5dc5ae5dc7ae5dc8ae5e14ae5e50ae614aae5e91ae5eb0ae5ebfae5ec1ae5ec7ae5ee0ae5f51ae6194ae61c5ae61ffae6201ae6237ae63ddae6473ae65f6ae6902ae6915ae695eae6960ae6965ae6a28ae6a29ae6a35ae6a81ae6a93ae6aa8ae6b9aae6b9eae6babae6c6aae6c7cae6cb5ae6cc0ae6cd2ae6d40ae6d76ae6d86ae6da6ae6da8ae6dafae6db8ae6dc9ae73efae7427ae746fae7482ae74aeae74e4ae74eeaf551baf9babc2bcabe2001ce200aee4007ce4008be400c2e40142e48c6ce49174e49af8e84930e84931e8800401007902005e0200610200ee0344450640f206a3930901380940130ac1520ac3c40ac7c90ac7db0ac8280ac88e0ac8960ac9260ac9400acaf70b603e0b603f0c215a0d08a80d0bf4142da0142f6414a127150099154c831553411d334532000f32003b32005833fd1033fd3b33fd4f33fd6d33fd8033fdc233ff1a3532cb3532cc35350c3571c53591c63593cc3aaabc3aaae63aab243aab263aab333aab4b3aab603aabf23b76003b762e3b7b5d3b7b6a3b9bae3b9be33e857e3e8ef23e8f0f3f43e63f62193f86dc3f89753fbba93fbc8343c17443c1e243c25843c2aa43c32743c39343c39c43c40343c42143c4c543c5e443c5f543c66d43c68a43c6da43c72943c79d43c7ea43c80943c8b943c8c043c8c343c94843cf2e43cf3a447d2644c1e444f66d44f67644f67b457c2045f446467801477f7147812448041c48051248080248083748083a480c4548d84648d88648da61497c03497c0649842c4984614984d24a34e14a35b64aecb94b7f8f4b7f964b7fc34b7ff24b82254b822a4b82c44b82d54ca13e4d03d04d20de507f9d50ff3e6008816831f77102a171030471030e71fe5c743591763b2c76e31076e72b7a424d7a42547a431a7a438b7a443a7a49cf7b059f7b704f7be04e7cf8357cf83e7cf85d7cf8897cf8ee7cf9957cf9ab7cf9b38002448002658002688002c98002f7800332800e3e800e8b800f338016ec87c00287c40c87cc1888140b881b668880bd896c2f896c628991828a041b8a042b8a042c8a055d8a07088a0935a7cabea7ce16adfc6aadfc8cadfcbcadfcbfadfcd5adfcecadfcf3adfcf4adfcf7adfde6adfe10adfe15adfe65adfe6fadfe92adfebbadff08adff58adff7dadffa0adffe0adffe6ae0013ae006bae007dae00a1ae00b3ae00c4ae00daae0164ae01a4ae01f9ae023bae023eae0244ae025aae02caae03c2ae03ebae0406ae0418ae044dae0488ae048aae04bcae04c7ae04cbae04f8ae0504ae0509ae0559ae0563ae0565ae0635ae066fae0690ae0691ae0692ae06d9ae07adae07cbae07e5ae07eeae088bae089cae08eaae094fae0950ae095bae098eae0993ae09a8ae09d4ae0a09ae0a12ae0a1fae0a8dae0aadae0b08ae0b1eae0b20ae0ba9ae6c02ae0baaae0bc2ae0c0fae0c39ae0c43ae0c4fae0c71ae0ca5ae0ccdae0d1cae0d90ae0dc6ae0dc8ae0de6ae0e29ae0e34ae0e35ae0e4bae0e62ae0e64ae0ebaae1119ae111dae1146ae11d5ae11d6ae11e2ae123dae130dae139fae13c4ae140fae14c5ae172cae1732ae173fae1789ae17a4ae17aeae18f6ae1976ae19e8ae1c07ae1c08ae1c32ae1d19ae1dc4ae1dc8ae1e60ae1e77ae1e86ae1eabae1f1fae1f27ae1f60ae1f76ae1f9bae1fa8ae1fc1ae2024ae2054ae20c5ae20fdae210fae21d6ae24c5ae26abae26caae272dae2742ae2770ae2771ae2772ae2937ae293bae29cfae29daae29fdae2ef2ae2ef3ae2f9fae479eae47e4ae4829ae487bae487dae488dae49c2ae4a0dae4a1eae4a4cae4a53ae4a94ae6c58ae4b80ae4bd7ae4d32ae4dfeae4e14ae4e92ae4eefae4f3cae4fc5ae50b2ae51a3ae51a8ae51b9ae51bbae51ceae5282ae5289ae52ccae5373ae5383ae5393ae539bae5421ae54e3ae5648ae5661ae566eae5694ae56c2ae56eeae5718ae571cae5726ae5783ae57cbae5870ae587aae5899ae58b5ae58b6ae58edae58f4ae5941ae599aae59c7ae59cfae5a0aae5a42ae5a4dae5ac6ae5acaae5b16ae5b80ae5bd5ae5c77ae5d53ae5dccae5e16ae5e97ae5eabae5ed5ae5f0eae5f4eae5f92ae5f97ae5f9cae5fa1ae6042ae60b0ae60cbae6203ae623cae6334ae638aae63deae63edae6413ae6475ae647dae64b7ae64c3ae65f5ae68bdae6989ae698dae6a14ae6a2dae6a3eae6a7cae6a80ae6aa9ae6b9dae6bcaae6bf3ae6d3fae6d7bae6da4ae73ebae748dae74b4ae74b7ae74c2aed61daf09c4afc66ac2b08fc87f2dc87f36e14d34e400b0e847f8e8cfb9e8cfbfe8cfce0101780101910200b70200c102010706a26306a2920a40050a400b0a40230a405e0ac3180ac7e20ac89e0ac8d90ac9860acb170d09140d0b280d8212142280142d8614b67014b67314b8ee14f11b1533ba154c8f1d333d32001332002432006133fccb33fd7633fdb433fdbc33fde533fe1433fe2e33fe9d33ff1833ff1c33ff8c33ffb53426933474d23532c73532ce3565953571d43593cb39a85539a8573aab033aab643aab7d3aab813aabca3aabcb3aabd83b769c3b77903b77aa3b9baa3b9bb93e854f3e95dc3f6eea3f6f023f793f3f88563f892a3f96663f97fd3f9e5b3fa1963fa9363facfe400ee243c06a43c14443c1d543c1fa43c20c43c22543c31843c38c43c39643c40843c4db43c55343c73b43c76043c76143c79343c8bf43c90f43c93443c972447d0044c1e844f64944f662457c0d46789c46f801473c07477f7b47819d4804124804444804914804b548080848084f480c2148d82148d84948d8e3497c4a497cb34984a54984a64984c84a35a44a81f84b7f5f4b7f8a4b7f9a4b7fc14b7fdd4b7fe04b82a14b82b14b82e14b82ea4ca31a505f6d506e1d506f45507f8650ff39702c3170c08d71002871025e71026d71034e71039d7103b671dd1871f88075032875032976209a76e7327a42227a42897a43fa7a49c47be0517cf8377cf8437cf86b7cf8937cf9a57cf9aa7cfa0c7cfa3380026a80027e80029f8002a78002ca8002da8002ea80041480079480079b800e15800e2f800f3087cc2687cd018805b088140688140e881bb8881bb9881bc78967d08967dc896c658a02d88a09378a0a36ae4fdeaa94bcadfc6fadfc86adfc87adfc99adfca2adfcb5adfcbbadfcc7adfceeadfe1cadfe95adfeccadfed4adff4aadff53adffb9adffc8ae0017ae0021ae0040ae00d5ae00e9ae0149ae015dae01cfae01d8ae01e4ae0229ae0256ae0271ae02f9ae030cae0319ae0320ae0389ae03caae03d0ae046cae047eae0483ae04b1ae04b3ae04b8ae04d9ae04daae04e2ae04f9ae0599ae062cae0659ae065eae0666ae068cae06e9ae07acae07feae0809ae080aae0817ae0875ae08b3ae08b5ae08fbae0944ae09d9ae09fdae0af5ae0b94ae0bbcae0bc3ae0bffae0c5dae0cd0ae0cd7ae0d6bae0d71ae0d8bae0d94ae0dd0ae0de9ae0e19ae0e22ae0e4dae0e4fae0ebeae1163ae116eae1170ae11b7ae11dcae11ecae11eeae1233ae1275ae1397ae13afae143cae1481ae149cae149fae14afae14c3ae14c7ae1525ae1577ae15a4ae171eae17b4ae17bfae1869ae1901ae1920ae1926ae1997ae19a6ae19c4ae19d3ae1b6dae1c11ae1d81ae1e4bae1e66ae1e71ae1e73ae1ea9ae1eb7ae1ec7ae1f1eae1f28ae1f4eae1f5eae1f82ae1f9aae1fa1ae1fe8ae1ff9ae2013ae2023ae2026ae2027ae20fbae223aae24e2ae24f7ae266fae2679ae267eae268fae26a2ae275fae2784ae27a2ae27a7ae27f4ae27ffae2902ae2919ae29d8ae29d9ae2f10ae2f18ae2f25ae2f5bae2fadae47a1ae47b6ae47e3ae4814ae4a7fae4b56ae4ceeae4d50ae4d64ae4d68ae4e1bae4ec9ae4f5aae4f77ae4fc4ae4ffeae50d9ae5114ae511eae51b4ae523bae5299ae529cae52c0ae52c7ae537dae539cae539dae540cae54e6ae54ecae54f5ae54f9ae5623ae5678ae56a2ae56b4ae56bfae56d4ae56deae5722ae5780ae5782ae578bae5795ae5797ae57a9ae57aaae5889ae5897ae5973ae59b9ae59e4ae5a2eae5a4cae5ab1ae5aecae5b3bae5b40ae5b6bae5b8bae5bbeae5bc5ae5bd2ae5c62ae5c6aae5c96ae5d17ae5d48ae5e7eae5ebcae5ec6ae5ee9ae5eefae5efdae5f3cae5fadae6018ae6028ae60c8ae60d3ae622dae68b2ae634bae6371ae6375ae63bfae647fae64a5ae65f9ae68aeae68bcae6918ae699aae6a1cae6a33ae6b9fae6c46ae6c5fae6c6dae6ccaae6cf3ae6cfcae6d7aae74b0ae74d0ae74e9ae77faaf6c50af8486c2b003c2bd2d33fdd5c87f11e4009be400f7e8066be847f4e8483401007c01008d0101900200bb05cc10062f0f06a2480940140a40120a401e0ac3c30ac7450ac7d90ac9330ac93a0d082e0d094414b9331501c0151cd8151cfa1526da152882152bc71533f31553491f2bca1f33951f6fa932001e32003332003c33fcd133fcfd33fd0333fd5a33fd6c33fde233fe1233fe9e33ff1f33ffcf33ffd43425993430853571cc3591d03591d939a84d3aaaa83aab0b3aab5b3aab7f3aabdb3aabdd3aabf03aabf73b769d3b77743b7b643b9bb243c7093e9cd643c70e3f43d43f4d0c3f66823f79f93fb7973fb88a43c00943c04143c17d43c1fb43c1fc43c22d43c24a43c31c43c31d43c32243c49a43c4bd43c4da43c55b43c56643c5dd43c5e843c5ee43c6a643c6e743c8bd43c8cf43c8f543c943447d04447d24447d4544c1e144f12844f68145f42c4680754682f046fbe947819f4804094804154804224804ad48086d480c01480c03480c26480c27480c2948104648312348d8a948d8ee497c9a497ca1497ca3497cc8497ce449842d4984394984474984ae4a35b74a35d14a81874a82f34a83094aebd34b7f5a4b7f5d4b7f674b7f714b7f804b7f8e4b7f914b7fa24b7faa4b7fc04b7ff44b82054b82154b82a34b82a54b82ad4b82df4b82e94c01554ca1e84d03cb501f9d506f675100e76831f6702c3070626170626871013771035071f9c2738a907434ce79141c7a42217a42587a429b7a44557a49377cf86f7cf8917cf8f17cf9e480022980024f80025e80026980029a8002a28002b68002b7800f0587c84087cd0287cd2e881b86896c3e8a04278a0a4cae0ddbac17c9adfc96adfc9dadfca5adfdb8adfdd3adfdeaadfe49adfe67adfe91adfeadadff62adff7aadff87adffafadffb0adffc6adffd1adffe2adffe9adfffbae0039ae003bae0061ae0086ae009bae00a5ae00b4ae00b6ae00ceae00d9ae0100ae0146ae01abae01b0ae01cbae01d1ae01f2ae021bae025fae0263ae02fbae0369ae03e2ae0424ae045fae0466ae046aae047dae0485ae048cae049bae04aaae04ceae04f1ae04fdae0586ae058cae05acae07eaae08acae08f4ae095fae09bdae0a8bae0a93ae0acbae0b3eae0b76ae0bafae0c0cae0c0eae0c23ae0c49ae0c5bae0cbeae0cd3ae0d0bae0d33ae0d4eae0db4ae0dccae0debae10b5ae5290ae10efae111bae1137ae1140ae1194ae11e5ae1258ae12b0ae13c0ae13f1ae141fae1423ae1445ae1451ae16e3ae172dae1740ae1749ae17a6ae1829ae1911ae1b61ae1b6eae1c2dae1d71ae1d7aae1e43ae1e57ae1e58ae1e7dae1ea5ae1eb0ae1f38ae1f54ae1f63ae1f70ae1fa9ae1fb7ae1fccae1fd5ae1ff6ae2009ae205bae20c4ae20eeae214bae2174ae21e0ae21f1ae24a8ae24e3ae24f0ae24f2ae266bae2672ae2686ae2693ae26afae274bae2ee2ae2ee4ae2ef7ae2ef8ae2f5aae47b4ae47bbae47faae4825ae4832ae4842ae4847ae4863ae4876ae4a7eae4ae6ae4b33ae4bacae4be7ae4d33ae4d69ae4e99ae4ec7ae4ee6ae4f54ae4facae4fecae4ff6ae5156ae519533fcb6ae51d8ae528dae52f4ae5310ae5377ae5396ae539aae53d2ae54cdae54dcae54e4ae55d5ae5660ae5685ae56d7ae56e1ae5721ae5772ae578aae57a3ae57d7ae5885ae58a7ae5967ae5990ae59b8ae59c8ae59d0ae59d3ae59eaae5a63ae5a6fae5a94ae5a9fae5aa4ae5accae5adeae5b1bae5b3dae5b68ae5b81ae5baeae5be2ae5bedae5c5eae5ca6ae5cadae5cc9ae5ccfae5cd7ae5d1cae5d50ae5d54ae5d85ae5ec9ae5ef7ae5f80ae5faeae6004ae6026ae6029ae6035ae61faae620dae621aae621dae622fae634dae637dae63b9ae63bbae63c0ae63e3ae63ecae6479ae6486ae6959ae695fae699bae69c7ae6a1bae6a86ae6c22ae6c61ae6c67ae6c79ae6cbcae6cbdae6d81ae6da9ae6dc5ae6e80ae73d5ae7475ae749cae74c1ae74c6ae77d4ae77f333fd19c2b3a5c87f2ec87f3be20008e48443e48c6ee4916ae49217e4992ee8068ae88d54e8cfc30060370100710100800200390200b202013106800206a20c08af3e0a403c0ac3410ac7d30ac7d80ac9340ac9c70acafa0b41160d05e60d07db0d08aa0d08b50d0e26142c81142ec114f91f1500c5152884152895152be8154f531d333c32000732004132004f32005733fdde33fde033fded33fe0e33fe1e33fec83426943430d83442c43571c43aaab73aaad43aab083aab213aab2a3aab403aab473aab613aabdf3aabe43aac273b75c03b76503b779f3b7b6b3b9bb73b9bce3b9bf73e83d43e8e963ebd413f69313f8fbe434ca943c0e243c10f43c14b43c17343c25643c28f43c42443c45243c4c043c56243c6a043c6bb43c6cb43c6f543c6f843c70843c71043c7e043c8f143c92e43c93743c94a43c95544c1ea44f0a745f44846789746800746807b4680fb46815e477fc9477ff048040e48043e48048048048448080648083c480868480c2b48d84b48d88d48d89648d8ad48d90648d90b497024497ca5497cb2497cb4497cb74984584984894a34d84a82204b7f8c4b7f904b7fc64b7fce4b7fe54b82134ca1e94d03c34d232d503fd9516203600b5d68324c702c0270625f7102a97102de7102e07102fc7102fd71fa0671fa11738a53738a6374359475012d75015676103076105176e72678044878ccaf7a426c7a445f7b700a7bb17e7cf8477cf8347cf84d7cf8737cf9257cf9917cf9a07cf9ad7cf9b47cf9d67cf9fe7cfa167cfa4f7cfa8380027380027c80027d80033180033687c84e881407884b01896c12896c438a032a8a06578a0842ae6bb5adfc8fadfcb1adfcc5adfcddadfdc0adfddcadfe19adfe75adfea8adff42adff5dadff63adff6eadff71adff7eadffc1adffc7adfff7ae0029ae0033ae0042ae0043ae00aaae00c7ae00cbae0206ae0262ae0267ae02d4ae0329ae037aae03e5ae03edae0402ae046eae0472ae049fae04bbae0501ae0566ae056aae05ddae0654ae0667ae07f5ae0874ae08a8ae08e2ae08edae093aae09abae0a32ae0a4bae0a78ae0a86ae0a8fae6b92ae0ab2ae0af4ae0bceae0bebae0c0bae0cc4ae0cc7ae0cc8ae0d35ae0d8dae0db5ae0dc2ae0ddcae0df9ae0e2aae0e2dae0e30ae0e41ae0e86ae0ec5ae0efcae10beae1107ae110bae1112ae1123ae1130ae113aae11daae1201ae1235ae1240ae1254ae138fae13a9ae13c2ae1439ae144aae1455ae1469ae149aae14ffae1736ae1742ae17b6ae1839ae184fae1850ae1864ae186bae1871ae18ccae191fae1925ae1b8eae1bf5ae1c38ae1dbdae1e5fae1e89ae1ec3ae1ed5ae1ed8ae1f35ae1f53ae1f5fae1f6aae1fa0ae1faaae1fcfae1fd9ae1ffdae2000ae200cae2137ae222dae2604ae267fae2edbae2eddae2eeaae2ef1ae2f57ae2f58ae2fd6ae478eae47feae4819ae4862ae4866ae499dae49c1ae4a4bae4aeaae4af2ae4af3ae4b6cae4b6fae4babae4d54ae4e00ae4e0dae4e1dae4eb3ae4ebaae4fb2ae4fd5ae4ff1ae50b1ae5193ae5196ae52aaae52baae52caae52f8ae53c1ae54e7ae54f4ae54f8ae564cae567dae5686ae56b0ae56c8ae56dcae56e5ae572cae5778ae577cae57d5ae57d8ae58e4ae595fae5965ae59afae59bdae59c4ae59c5ae59deae5a01ae5a1cae5a2fae5a47ae5a54ae5a59ae5a68ae5a8eae5abfae5ad2ae5b6aae5c5cae5c60ae5ca4ae5cabae5d1eae5d4eae5eb1ae5eb5ae5eb9ae5effae5f14ae5f8fae6019ae601fae60b2ae60f0ae6332ae6346ae6353ae636eae63bcae63d8ae63daae63e1ae63e8ae63eeae6484ae6961ae698bae698fae699cae6a13ae6a1eae6a36ae6a73ae6a7eae6aa6ae6be7ae6bf9ae6c45ae6cb7ae6cf1ae7458ae7476ae7478ae74bdae74ceae77a7aed1cdaf3c55af8602afca18c2b03fc2b071c2bdc3c2c07fe4009ee49254e49929e84035e847f7e8c00702006003e32506a21206a24e06a26506a26c0a40110a40350a40550a40570ac38e0ac3c20ac7440ac7460ac82f0ac89c0b44530c4053142dc7142e3b142f6914f126151cd3151d0b152bc91533111533be1533fa15405b1b4f5f1d33431d334632000b32001132002c32003232004c32005633fc8233fc9833fceb33fd0833fd4633fda033fda533fde733fe0133ff9233ffc533ffe53592133aaaa23aaaa43aaad73aaada3aabe23b76243b77403b777a3b77b73f722e3e89983e8b023f4ba93f55723fa9ff3fb37b3fbdfc43c22243c27343c27643c29143c30443c38e43c45143c55643c55d43c56443c61143c68f43c69c43c6dc43c6e943c76543c76b43c78043c79443c79943c87f43c8eb43c8fa447d1844f67e44f69244f6a4457c1746788b46789e46789f4682c046ec4b473c08477f73477f9148042948044348044b48083b48084b48d8ae497c014984274984594984b54a35ef4a822a4a83144b7fd14b7fef4b801f4b820b4b82a84b82b04b82bf4b82d24b82da4b82e7505f74506f24506f41506f64506f6b507fa970c09171026771029171029d7102a27102d571030f7103b271041e7281317434cf74368c75032a76410376e30b76e725777e0d7913697a42017a42877a42a67a49c37a49da7bb1557be01f7cf8397cf8587cf8647cf8657cf8787cf8827cf8837cf8847cf8f27cf9dc7cf9fa80024b8002778003c180041580147780150f87cd20881b6b884015896c2b8a08cfe20007ae04fbae058dae0590adfc6dadfc84adfc97adfca1adfca7adfca9adfcf0adfcfeadfd08adfd6fadfdc6adfe1fadfe8fadfea2adfeacadfebfadfef2adff57adffd2adffebadffedae004aae00b0ae00d8ae00fbae0101ae012eae0131ae0132ae0134ae0145ae0148ae014bae014cae014dae0153ae01b3ae01c7ae01d2ae0204ae021fae0232ae0246ae0260ae026bae02d3ae031bae032bae035cae037fae038aae03feae0410ae04adae04c5ae04e3ae05b9ae05d4ae05e4ae0608ae0623ae0626ae0672ae0687ae0798ae07a8ae07b7ae07bcae07d2ae0813ae0832ae0840ae0842ae085bae0892ae08beae0940ae09adae09d2ae0a10ae0a55ae0a92ae0aaaae0bb5ae0bedae0c25ae0c37ae0d12ae0d34ae0d51ae0d68ae0de1ae0de5ae0de7ae0df0ae0e38ae0e46ae10f2ae111aae1165ae1171ae11b3ae11beae11caae1293ae138dae13f8ae13fcae145fae1464ae14acae14b0ae14bcae14beae1597ae16edae16fdae1746ae174bae174eae17a9ae17b7ae183aae18e7ae197cae19caae19f0ae1c2bae1c30ae1d03ae1dc1ae1df0ae1e94ae1eb4ae1f4bae1f91ae1ff4ae201eae2025ae202aae21ceae21e7ae23d4ae23ecae2657ae2690ae269fae26bfae2791ae29d0ae29d1ae29d2ae2eedae2efaae2f5dae2f5eae2fd4ae35cdae47baae47ddae47ecae47f9ae4812ae483aae4848ae4861ae4889ae49f2ae4abfae4af0ae4b34ae4ba9ae4beaae4cf9ae4eaeae4eafae4ed8ae4eecae4fafae50c4ae50ccae51cdae5263ae5267ae528eae5291ae529aae52a7ae52bcae52cdae52f0ae5306ae5363ae5379ae5387ae5391ae53d8ae54e5ae564dae5675ae5690ae5698ae56a3ae5792ae5794ae57a2ae57acae57bbae5887ae58e2ae58e6ae5985ae59a8ae59e5ae59fbae5a58ae5a81ae5ab6ae5addae5ae7ae5b28ae5b67ae5b76ae5bc4ae5bd8ae5cb2ae5cd1ae5cfaae5d0eae5d4fae5d67ae5dd4ae5df2ae5e9fae5ecbae5eceae5f13ae5f44ae5f56ae5fa7ae602fae60b1ae60f1ae60f6ae6169ae6193ae61fbae6213ae6214ae6219ae6226ae6227ae622cae626bae628bae632bae633dae6349ae6385ae63e5ae6401ae6406ae6474ae6493ae68b8ae68bbae6a18ae6a19ae6a3bae6a6dae6a99ae6ac3ae6bebae6c13ae6c14ae6c17ae6c6cae6c73ae6cacae6d53ae6d56ae6d58ae6d5aae6daaae74a7ae74abae74baae74cbaedae5c2bdffe483d1e48f41e84800e84802e8490d01008c01016602010a038f4f06409806a24d06a25906a27606a29a07007e0982250ac0f60ac7510b43000c404e0d05470d065f0d08a90d09420d09610d8032142f5c14f1191500941501c815287f152888152bcb1533c8154c5c155335155342155353155356197cd032001b32005032007b33fc9633fd0e33fd4a33fd4b33fd6533fdab33fdcf33fdd333fe1333fe2233ffb333ffc33424533530413530553532c33aaaa73aaaa93aab653aab9b3aaba03aabd63aabe13aabe73b761c3b76313b764c3b76cb3b77853b7b6f3b7b703b7b723b9bef3ea74d3ead633f54073f67323f6e573f7b3c3f7dc13f7dec3f8c413fa9413fbd82405f354060d643c04c43c08143c0ae43c16343c27443c27543c39243c46e43c49d43c4c243c4d143c55e43c5ef43c61443c68043c69243c75943c7ac43c7ad43c7ce43c7e243c87e43c8d543c8e943c91e43c93a43c94b43c951447d23447d6944c1e544f64544f64c44f66f45f4374678954680fa46ec4c4781a048041a48050d480513480c4848104148c15e48d82b48da6348da83497c09497c2b497c334984214984954984b04a34e94a360e4a81f44a821f4b7f9f4b7fc54b7fd34b82144b82184b82d9506f23506f6250ff3a516206600bb76831ce683307702c4670c0817102807103007103ad71d87171dd1771f90371fa10738a49738a62738a647586247610477a43f27a49487bb0e27bc7b77bd0367bd04c7be0297cf8317cf8717cf8757cf88d7cf8d77cf9ae7cf9d47cf9f27cf9ff7cfa1b7cfa4680027a8002958002ce800323800dbb80150e87c41187c835880404881bba89602a896c0f8a00248a090cae4d65ae779eae0a77adfc88adfcedadfd0fadfdceadfe60adfe71adfeb3adff6cadff8badff99adffc9adfffcadfffeae0007ae0015ae0032ae0041ae008eae00a0ae00a9ae0133ae013cae014eae0152ae0192ae01acae01f1ae0201ae0202ae0224ae026dae02d9ae0358ae0371ae038eae03ccae03e6ae03e7ae03e8ae0411ae0417ae041cae0440ae0446ae044bae0455ae0473ae04baae04e4ae04edae0506ae057eae0591ae060dae061dae0622ae066cae0676ae07ceae07d4ae07f2ae0801ae080bae080cae088cae0977ae099fae0a18ae0a2bae0a89ae0af3ae0b04ae0ba6ae0bb6ae0c08ae0c98ae0ca8ae0cbdae0cc9ae0d03ae0d93ae0e15ae0e71ae0e85ae0ea7ae10c1ae110aae112bae112fae1142ae1143ae1144ae11bfae11d3ae1204ae1239ae1241ae1274ae12a1ae12a8ae12acae13bbae1419ae1442ae1533ae1536ae1595ae15c4ae16f3ae1753ae1793ae1870ae1905ae1927ae193cae198aae199cae1ae8ae1befae1bf6ae1d1cae1db0ae1e4aae1e6dae1e78ae1e82ae1ea6ae1ec0ae1f37ae1f5bae1f7eae1f8aae1f8dae1f90ae1fd3ae1fe2ae1ffaae202eae203dae205aae20b6ae21e9ae223eae24e9ae2656ae266cae26d4ae5140ae2f1aae2fa5ae47a0ae47b0ae47b5ae47e1ae47f2ae4831ae4840ae486aae4881ae4d2bae4d30ae4dddae4e8fae4e98ae4ea5ae4ea8ae4ebcae4ec8ae4f16ae4fb8ae4fd4ae4fe6ae4ff4ae4ffcae50c5ae50e1ae5183ae519aae51a9ae51acae51b0ae51bcae51c7ae5254ae5261ae5274ae5288ae52d2ae52f7ae54b6ae5665ae568dae5871ae5880ae588eae58c0ae58c7ae5961ae59aaae59c0ae59d7ae59f3ae5a08ae5a0dae5a1fae5a7dae5a82ae5a92ae5ae0ae5b2eae5c73ae5d25ae5dfaae5dffae5e01ae5e08ae5e11ae5ec4ae5ee1ae5f10ae5f4fae6012ae6014ae6017ae602aae61b8ae6205ae6231ae6240ae6328ae632dae633eae6372ae6381ae63b2ae63c2ae63e7ae63eaae63fdae63feae63ffae64b1ae64bbae6901ae6903ae6996ae6a3fae6a7aae6bc6ae6c0cae6c0fae6c4cae6cf2ae6d41ae6e7bae73d6ae7412ae744cae747dae74aaae74c7ae74c8ae74e73f8686e20017e2005ee4009fe4015ae48e5de494a3e4955de8cfbee940500101a00200a20200fd0640f106a21406a21706a21e09012b0940100a40180ac3150ac7a40ac9a00ac9e90ba0090d01e03f9359142deb14f90d1528891533e11533f6154ee715534f15537e1d334932000632001232001d32002232002732003832007e33fcbd33fd6433fd7433fd9b33fdb533fddd33fde633fe1933fe7933fe7a33feca33ff4b33ffc634308634738a3aaac93aaaf03aaaff3aab013aab593aab723aabae3b75b53b76373b76463b76663b76683b76743b767f3b77733b777f3b77803b7b853b7bf93b9be73b9bfb3b9bfc3e90c83ebd9a3f43463f56e63f66b03f853b3fbdc443c06d43c07043c0bf43c17243c1a243c25043c26e43c27743c2b043c30843c32843c42d43c4d943c60e43c61a43c68143c6df43c6eb43c6f743c6fa43c73f43c7da43c81043c88043c8c443c8f943c92643c94943c96243c96543c99f447d3a44f02844f16744f1a544f42b44f845451c4245f42646812e477fd348040648041f48042b48042c48048148048d48049b48083648083948084a48104348105448d88548d894497c27497c73497c784984534984544984564984994984b34a81e64b7f7f4b82044b820a4b820c4b82974b82dc4b82e04b82e24b82e34b83a94c2c2a501fbc506e21506e22506f5e6831f4702c66702c67702c8871f88371fa0f71fc02738a0474358d76105d777e0b7a426b7a42a07a44407a495a7a495b7cf83c7cf8427cf8857cf8907cf9967cf9e97cfa027cfa317cfa3780020a80022580026d800270800790800e13800e54800e63800e71800f3480171687c40e87c82287c82487c84d87cd1b881409881b61881ba68880d1896c0d896c4889900289910f8a06eb8a08668a08918a08948a08dc8a2907a1b361ae1120ae5284ae068eadfcd8adfcf6adfd0badfdc4adfdd5adfe1eadfe4dadfe54adfe6eadfeddadff07adff40adff59adffcaadfff9ae0005ae001aae003dae0056ae0074ae0079ae0099ae00e7ae010eae0113ae0147ae01faae0238ae0261ae036fae038bae03deae03f9ae040eae0422ae0428ae044eae04c0ae04caae04f2ae059dae05a5ae0618ae061eae0655ae0669ae07c1ae07c2ae07e6ae0806ae08adae60e9ae08dcae08feae09a4ae09b5ae09c4ae0a35ae0a49ae0a53ae0ab6ae0abeae0acaae0aedae0b35ae0b3cae0b71ae0b8aae0b92ae0b9aae0bd7ae0c14ae0c33ae0c9aae0cf5ae0d37ae0d6aae0d6cae0dbfae0de8ae0e2cae10e2ae111eae1158ae11b6ae11c0ae123fae1276ae138aae13cdae13faae1416ae1446ae144bae1460ae149dae14adae14c1ae1517ae1534ae1729ae178dae185eae198bae198dae1a05ae1ba9ae1cc8ae1cf9ae1d8fae1d91ae1dc7ae1e4cae1e4eae1e5aae1e6eae1f48ae1f6bae1f6cae1f71ae1f8bae1f93ae1f96ae1fbdae1ffeae200eae2044ae2052ae2060ae20bdae20c2ae2131ae216fae2486ae265eae2675ae2681ae26b2ae2730ae277dae29ceae2bf9ae2bfaae2ed8ae2eefae2fa4ae3409ae449bae47a5ae47e0ae4822ae484bae499fae49e1ae4a16ae4a21ae4ae0ae4af6ae4b83ae4e16ae4e1eae4f23ae4f26ae4f42ae4f8fae4f94ae4f97ae4fe5ae50dbae5119ae514eae51b7ae51bfae51dbae5275ae5312ae531dae5360ae54dbae565bae565cae5670ae56b1ae56c6ae56e0ae577fae5781ae57abae57b5ae587dae588fae58e5ae58eaae598bae59b0ae59bfae59d8ae59e2ae59e3ae5a19ae5a77ae5a7eae5a85ae5aabae5b2dae5b3eae5b47ae5b71ae5b7fae5bc2ae5c17ae5c99ae5c9bae5c9cae5ca2ae5d15ae5d38ae5d84ae5d9bae5dfdae5e03ae5e4cae5e65ae5e9bae5eb3ae5f39ae5f3fae5f45ae5f94ae5f9fae5fe7ae6006ae602bae605fae6060ae60d1ae61b6ae6229ae6329ae6354ae6376ae6384ae63f9ae6499ae64a8ae65f8ae68afae6904ae6a12ae6a7dae6ab9ae6bacae6c18ae6c34ae6c68ae6c6bae6cb1ae6cc8ae6cd7ae6d74ae6db2ae6e76ae6e7eae73e5ae7457ae74caae74e1ae77c4af4fb3afb23cc2b067c87f09c87f24e20022e20094e4014de402d0e47e40e48bd9e4916ce49273e49d16e80688e8068be8800ae940fae940fb0200ff02b26d04c20d06a21106a26906a27b0a40060a40090a401a0a401c0aca230b41170b42270b42900d05c20d06da0d08240d09aa15008f152be215406415534032005132007733fcfe33fd0633fd2433fd4d33fd6833fd8f33fdf533fe0833fe0d33fe1d33fe1f33ffae33ffec34264f34269734314c3432ce3434d634360334738f3532c43aaabe3aaaf63aab233aab633b75463b765a3b766a3b773f3b7b6c3b9bb83b9bd73f52cd3f89053f8c623f99c63fbe24400ee843c40743c42b43c00043c09943c13e43c27243c31f43c36b43c39843c39f43c3a143c42f43c46343c49b43c4a943c5eb43c5f743c67743c67843c69443c6bf43c6fb43c70a43c76943c76c43c7a243c7a543c7d343c7d743c93943c964447d1a447d31447d3f44f14644f18144f1a144f42144f60144f60544f67c457c21457c3145f42b45f44246810246815a46831346ec4d473c06477fd047812747812e48044148050948051448080148084d48c0de48d8a848d982497c714a34e34a35d54b7f454b7f6d4b7f7d4b7f994b7fa94b7fbe4b7fc24b7fc74b82034b820e4b82cc4ca41c4d03c0501f9c501fba505f71506e20506e6c71027c7102a77102dc7102e171030771037f71039b71f01171f90571fa09744a6275002676104b76e31276e7247830707a42267a428a7a42b27a486d7a49317a49397aa30b7cf8547cf88e7cf8f47cf98e7cf99a7cfa737cfad280022680027180027f800290800318800602800792800dc0800dc187c81b87c83487cc0487cc098805a5881b6e881b83881b8988401a884b02888005888006894011896c02896c6b8a04208a042d8a08bd8a09e7a2de5cae1ae0adfc66adfcb0adfcc2adfcdeadfd04adfdc7adfdd7adfdddae6a74adfe13adfe4badfe5aadfe5fadfed8adff8cadff98adffd5adffeeadfff2ae001bae0026ae003eae0090ae009fae00cdae00d0ae00f5ae0200ae0239ae023cae025dae025eae02deae0359ae038fae03c4ae03e9ae03eeae040dae0414ae041eae0467ae0477ae04ccae04d0ae04ebae0576ae05a4ae0628ae066aae06e0ae06e6ae07b0ae07d1ae07e7ae0847ae0865ae086aae0881ae0894ae08cfae08e9ae08f8ae093bae096aae098cae09c0ae09f7ae0a19ae0a33ae0a42ae0a76ae0a7aae0a8eae0afcae0b48ae0b52ae0b95ae0c05ae0c13ae0c54ae0cd8ae0ce1ae0d0dae0d2eae0d62ae0d7cae0d9dae0db9ae0defae0e01ae0e3aae0e54ae0edfae0eedae0f02ae10bbae10e1ae10e3ae110dae116bae11bcae11efae135dae1390ae1394ae139eae13a5ae13abae13bdae13c1ae13cfae1402ae140eae1431ae1461ae14a1ae14aaae151dae152cae161cae16e4ae171dae1788ae178aae17a3ae17e8ae1857ae185fae1923ae1959ae1b4eae1baaae1c02ae1d2aae1d2bae1d85ae1db2ae1e6cae1e84ae1ea1ae1eb5ae1ec1ae1ec9ae1f3dae1f8cae1fb2ae1fcdae2011ae212cae21ebae222fae24eeae2673ae2678ae2685ae2687ae26a5ae2764ae277eae2798ae2911ae2939ae2bd7ae2c3bae2eebae2f2aae2f9cae2fa9ae47a6ae47a8ae47aeae47bfae4845ae485aae4864ae486fae499cae49c3ae4b00ae4b53ae4b77ae4b8cae4b8eae4b91ae4ba5ae4bb1ae4c62ae4d2aae4d5aae4e9bae4e9fae4eaaae4ebbae4ec6ae4ee5ae4ef0ae4f1eae4f7fae4fa1ae4fb6ae4fb9ae4ff2ae4ff5ae50b9ae5187ae519dae51b2ae51e2ae5238ae5287ae52a8ae52bfae52c8ae52d1ae5304ae530fae5376ae5394ae564aae566bae5671ae567aae5695ae569aae56e2ae5776ae5786ae57beae5878ae587cae587eae587fae5895ae58a5ae58f3ae5962ae5963ae59aeae59cdae59edae59f6ae5a46ae5a84ae5a88ae5a97ae5ab7ae5acfae5b27ae5b37ae5b72ae5b73ae5b77ae5b87ae5b8aae5badae5c5aae5c94ae5cacae5cb0ae5cb1ae5cd0ae5cf1ae5d0fae5d39ae5d62ae5d6aae5d75ae5dceae5df6ae5dfeae5e0dae5e13ae5eb4ae5ee8ae5ef9ae5f0dae5f19ae5f31ae6043ae61dcae6223ae633aae63b6ae63c3ae6603ae68baae68beae68c0ae6a20ae6a88ae6a8eae6a96ae6b87ae6baeae6c5cae6ca9ae6caeae6cafae6cdfae6da2ae6db5ae73e9ae7447ae744aae7466ae74afae74beae77daaf0a4daf351faf4fabaff009c2b02bc2bb7fc2bd41c2bd4bc87f02c87f28c87f3de2002fe40410e49592e49934e8066ce8cfc4e8cfcc01007a06a26206a2950941090a401b0ac37e0ac3ef0ac79d0ac9020ac9ec0c404914247e142f4f1432a814f1231501ad151cd2152b9e152ba4152bb01540661540731551e015522132000432001832002f32005933fc8c33fc9133fcd233fd8d33fdc633fe2433fe3a33fea933ff4733ff4f33ffd33426143474143532ca35350f3571c83571d73aaacb3aaad53aaae03aaaeb3aaaed3aab003aab3c3aab5d3aab6e3aab7e3aabf13aac163aac343b76723b77ab3b9bf93b9bfd3ea5413f80193f814b3f85ca3f8bd0400eed43c04e43c20743c21c43c25743c48e43c4ac43c5e043c61643c61e43c6a743c6db43c8d743c91f43c94743c97a43c98c447d1b447d20447d3444f02744f18744f1a244f64144f67745f436467892468158473c0a47811a47812f4781a14781a348040548044c4804b248050a48050b48083d48085c480883480c4148104248105148d82748d84048d84c48d8e4497c21497c42497c4b497c724984944a81f94a83064b7f444b7f664b7f744b7f984b7fa84b7fb64b7fec4b82164b82cf501d13502faf505f875100c2702c2670626c7102df71034f71039f71f80171f90971fa07738a8d7435897503f97585db764101791cad79c1047a427b7a493c7a49597b70267cf86c7cf9a27cf9ac7cf9eb7cfa348002eb800dc380145087cc3e881b67881bab883b86896c5c8a063d8a08d28a2902ae5e8cadfc7dadfc85adfc9aadfcc0adfce5adfce7adfd0cadfdccadfe86adfe8aadfe8eadfea4adfeb6adfedeadff3fadff4badff4fadff78adff80adffa2adfff5ae0053ae005eae0075ae0084ae0097ae00a3ae00f0ae0150ae0156ae0181ae0193ae01c4ae01e8ae01eaae0247ae037eae040bae0474ae0478ae04a6ae04c2ae04dcae053eae0569ae0571ae0588ae05afae05b0ae05e2ae0649ae0651ae066dae07a7ae07bbae07dbae08a2ae08a5ae08afae08faae0943ae0992ae0999ae09beae0b0dae0b15ae0b39ae0b9cae0b9eae0ba8ae0bc0ae0bc5ae0c50ae0c59ae0c5cae0c73ae0cbaae0db2ae0e39ae0e57ae0e5aae0e8aae0ea4ae0eabae10f9ae113eae1d5aae1199ae11abae11cbae13f7ae1454ae1463ae14abae14faae1547ae173eae1744ae1750ae1797ae17adae17f5ae1803ae186eae1936ae1943ae1949ae1962ae1980ae1b92ae1b9aae1c10ae1c22ae1c27ae1d5eae1d5fae1d88ae1db9ae1e48ae1e52ae1e64ae1e76ae1e8eae1e92ae1eb9ae1f3cae1f62ae202fae2063ae2140ae21cfae21d7ae2676ae2677ae2683ae26c9ae27f6ae290eae2923ae29dbae2ee0ae2f69ae2f6dae4789ae478cae47b2ae47f3ae4830ae4839ae499bae4a1bae4b3eae4b95ae4cedae4cf5ae4d5fae4de1ae4e9eae4f2dae4f5cae4f93ae4fd1ae4fd2ae50adae50e3ae51c3ae51d6ae5258ae527cae529dae529fae52b8ae52c1ae54b5ae54d4ae54e0ae564eae5650ae5688ae5693ae569dae569fae56d2ae56d9ae5744ae5799ae57c6ae58dfae596aae5981ae59b7ae59faae5a2dae5a3cae5a56ae5a6cae5a93ae5b44ae5b88ae5bb2ae5c5bae5cdcae5cfbae5d08ae5d45ae5d5cae5d65ae5d89ae5d92ae5dc0ae5dcfae5debae5df9ae5e15ae5ea6ae5ed9ae5ef2ae5f4cae5f53ae5fa8ae600fae6023ae605aae61f6ae61f8ae6216ae6335ae6356ae6378ae63f3ae6481ae6498ae649fae68b3ae6916ae6919ae6a1aae6a26ae6a3dae6a84ae6a85ae6aa2ae6c12ae6c1bae6c1eae6c82ae6cbbae6cc9ae6d72ae6dacae6ed7ae73edae74bbae74d1ae74ddae7798ae77d0ae77d5ae77fcae8953aed618aef550af4fa7af7831af85fbaf8603af9ba1c2b035c2b05dc87f0fc87f25e14c70e40550e48932e48e51e849c402003702003c02004c0200b60200bf0200e202012f03e3e604403e06a21c06a25e0ac8940acad80b41130c217e142e6714667214757e14a12c1500981501b815287c15288d152b051533e0154e6532003f32007933fcbe33fd1b33fece33ffcb3424903530423563413571d33aaae73aab073aab143aab223aab3d3aab913aab923aab9d3aab9f3aabb33aabc03aabc63aabda3e91ee3ea5bc3f447a3f44883f56853f619c3f6ee13f8d1f3fa2573fa5ea43c21643c22743c25a43c29f43c30c43c32a43c36c43c3a443c4e143c60b43c60d43c69b43c6c543c6d343c75f43c77b43c7db43c87a43c8ba43c8bb43c8d043c8d243c8f043c91c447d5644f14244f666457c0f457c3045f44345f4474780ee47811548083f480853480864480c0648d8567cf89c48d8a748d908497c11497c4049843149848f4984a24984a34a35a74a35c74a83024aecc94b7f624b7f754b7f944b7fc44b82124b82984c2c1b4ca13f50807f4d03cc503fdc505f6e505fa0505fa17cf8eb507f8750fd8a702c6870626d7103147103a8738a597434c374358274358a74359374359575050a76603876e72d78059d7a42497a429c7a42a47a49467aa03a7be9037cf86e7cf8707cf8967cf89a7cf9b17cf9c57cf9cf7cfa1780021e8002788002c78002f8800440800c3d800e1987c83f87c84c87cc0187cc2487cc4a88040188140d896c0b896c1e8a05608a05a58a0845a1b22eadfc74adfd00adfdcaadfdcfadfde8adfe14adfe61adfe85adfe9aadfeb7adfebaadff46adff4dadff74adffa8adffe7adfff8ae000aae000eae004bae007bae0081ae00c3ae00d7ae00feae013eae01c6ae01e0ae01f6ae01f8ae0219ae021aae0228ae0257ae02d8ae03d4ae03e0ae044fae0458ae047bae049cae04e0ae0503ae056cae0574ae0584ae058bae05b1ae05bcae06daae07dcae07f3ae07fbae0895ae089eae08a1ae08bbae0953ae09b3ae09c7ae0a2aae0a3cae0ac9ae0b6eae0b6fae0beeae0c27ae0c9fae0caaae0d41ae0d82ae0e52ae0e8bae0e9eae0eb8ae0ed4ae0ef9ae10b6ae115eae1172ae1197ae11bdae11dbae1259ae139aae13b5ae13efae1409ae145cae1478ae1704ae1708ae1718ae1792ae179dae17baae183cae18ebae1932ae1945ae1974ae1afbae1b8fae1d6cae1d92ae1d98ae1dbaae1dc3ae1ddbae1e91ae1eadae1ed6ae1f30ae1f34ae1f3eae1f46ae1fa2ae1fbcae1fcaae1fceae1fd8ae2007ae2012ae2061ae2068ae20beae20f9ae212dae21bdae21f8ae265cae26b0ae26d6ae26dbae291eae2befae2edaae2f6bae47e5ae47fdae4818ae4846ae488eae4a0bae4b30ae4be9ae4d49ae4d52ae4dffae4eb4ae4eb6ae4eb7ae4f12ae4f25ae4f92ae4fc1ae4fe3ae4ffbae50e0ae50e5ae510fae5121ae5148ae51afae51c5ae52a4ae52aeae52b9ae5320ae5365ae5366ae5372ae537fae5422ae54ccae54ceae54d6ae54f7ae56c5ae5752ae578fae57a4ae57b9ae57baae57d2ae57d3ae5890ae58f2ae599bae599dae59c9ae59d1ae59d9ae5a12ae5a17ae5a1aae5a1eae5a3eae5a5cae5a5dae5b3aae5bcfae5be4ae5c74ae5c8fae5c9fae5d51ae5d61ae5d69ae5d6bae5d7cae5de2ae5e18ae5e94ae5e99ae5ebeae5ec2ae5f15ae5f38ae5feaae6025ae6033ae6044ae6045ae60e8ae6224ae6235ae624bae633bae6391ae6393ae63fbae6400ae6482ae6491ae64c5ae65f7ae65feae6a79ae6bd6ae6bdcae6becae6c0dae6c59ae6cb3ae6ce4ae6d4eae6db1ae6e78ae73eaae73f1ae740cae74a6ae74cdae74d3ae77d2ae77f8ae77f9ae86acaecb47aef4c4aefcdfafa18bc87f03e400ece40135e483aae483abe48920e49210e49925e8064be8cfc9e8cfcf00cbac0200d5062f0106a21a06a26806a2720900fa0a400d0a400e0a40170ac82b0ac8910ac89a0ac8f40ac9270ac9e80b20140d04c20d08e4142c5e142d51142e96142ea915002515287b152887152b5815508332000532000d32001632001a32002533fc8d33fce933fd2133fd3333fd3c33fd4833fdbe33fe2633fea333ff1e33ff7c33ffbc33ffd83426163433923553163aaab83aaac63aaad33aaaef3aab0d3aab3b3aab833aab903aabc73aabd03aac373b76283b76473b76543b9bd13b9bdd3e920f3f402a3f57273f57e93f59f33f66313f80b13f81a53f84e83f9dac3fa48343c94543c02443c17643c22a43c25b43c26843c2a543c31543c43943c4c443c56043c68443c6c243c7e643c8b643c8dc43c8f243c94643c960447d06447d3244f63544f63a45f42845f44546788c473c03473c09477f72477fcb48042d48043d48080e48083848085f480c1b480c4448b0de48d85548da4248da84497fa149842b49844149847449848d4984924a34e64a35a64a35ac4a82ee4a830c4b7f894b7fa14b7faf4b7fb14b7fed4b7ff84b82014c5c00503fd2505fbb7062647103097103157103a471f00f71fc03738a617434c2750415758736762af37a420c7a424f7a425e7a426d7a427a7a43fe7a44447a444f7a44657a49147a495c7cf84b7cf8957cf9c97cf9ee7cfa307cfa448000788000e38002a580031e80033587c808881408881baa881bb3885337896c308991818a055e8a055f8a09328a093633fea833fec433ff1133ff75adfc98adfca0adfcb4adfd88adfdebadfdeeadfe0dadfe17adfe18adfe73adfed0adfedaadff10adff3cadff5cadff7cadff7fadff93adff9eadffcbadffd0adffeaadfffdae0012ae0025ae0063ae0078ae007cae00deae00f2ae00f4ae01fdae026cae02f0ae0324ae0391ae03d1ae03dbae03f0ae0421ae0453ae0469ae0470ae04a9ae04abae04bfae04f5ae057bae059eae05b7ae062dae0677ae0686ae06e5ae07bfae07d8ae07d9ae07dfae07e2ae07f8ae0807ae08d5ae093cae0952ae0978ae09c2ae09ffae0a7fae0aa8ae0ae3ae0b85ae0b86ae0b93ae0b9bae0bbfae0c21ae0c63ae0c65ae0c6cae0cafae0cbcae0cc0ae0cf6ae0d15ae0d26ae0d57ae0d6dae0d7dae0e51ae0e8fae10deae1129ae112aae1133ae1145ae114eae11e4ae127bae127dae145bae145eae14a7ae14feae151eae1531ae1535ae16e5ae1725ae172bae173dae17b2ae17beae182bae1836ae18e0ae18f5ae1991ae19c5ae1ba8ae1c0aae1c19ae1d46ae1d56ae1d94ae1de0ae1e6fae1e9dae1f6dae1f9dae1f9fae1faeae1fc3ae1fe1ae2004ae2022ae203aae2048ae212eae24e7ae2662ae2665ae266eae26daae2746ae2761ae2779ae2912ae29feae2ee1ae2f65ae47daae47f8ae481bae481eae4844ae484eae485cae4a2233ffd1ae4afaae4b47ae4b88ae4cf3ae4cf6ae4d36ae4d3aae4d48ae4de2ae4e0bae4e58ae4e91ae4ec2ae4ee9ae4eedae4f14ae4f31ae4f4cae4f80ae4f8cae4f91ae4fb5ae4fddae5004ae50b6ae50c1ae50c2ae50ceae50d6ae50e6ae5117ae511aae5120ae519eae51d0ae51e1ae525cae526dae52e4ae530eae53a5ae53d4ae5407ae541aae54c9ae54daae5655ae568aae56ddae5725ae576bae5789ae57b6ae5873ae589dae58adae58f7ae58f8ae5968ae59b5ae59f8ae5a71ae5a86ae5ab4ae5ae3ae5b39ae5bffae5c66ae5ca9ae5d14ae5d24ae5d28ae5d6dae5dc1ae5dd0ae5df8ae5e02ae5e09ae5e0aae5e0cae5e95ae5e98ae5eb2ae5ebaae5ecaae5ef8ae5f00ae5f06ae5f0fae5facae6005ae6016ae6022ae60edae60f3ae6212ae6218ae6220ae6249ae6336ae6345ae6380ae6394ae63c7ae63f7ae6492ae68b4ae690fae6917ae6a8fae6aa7ae6ac2ae6bb0ae6bc7ae6c0aae6c1fae6c5aae6cb4ae6d7dae743cae746eae748aae74ccae7800ae7813c2b09933ffd6c87f07e400a0e400b4e494a4e806b9e847f100c7f601007201007d0101d30200310200e50680b006a26006a26a06a27407007f0a401f0ab0280ac3f10ac3f30ac82e0ac8950ac8980b43560d08e50d0bf014311e14b5761500c1151cef1526e8152b0815407815520b15535215535731ff0a32000832002832003a32003d32005433fca233fcda33fd0533fd7e33fdc433fdd833fde833fe1733fe2833fe3033fe923422cc3533443aab293aab5a3aab753aab783aab8c3aabe53aac243b76553b7b693b7b713b7f753b9bd33b9bd93b9bf34a36073e87643f55ad3f5d913f60013f75883fafb63fb3a43fbebf43c76d43c7d8406f5043c07343c07543c22443c28743c29243c30743c31343c38d43c40f43c49743c6e643c74843c75d43c8db43c8ee43c96843cb58447d2244c1e344f0c944f1a444f652457c0545f42945f430467898477f8f48040248051048051148083e480843480879480c4048105348d88448d89048d8ab497cdc497cde49842249846f4984764984b44a35ae4b7f9c4b7fbc4b7fd54b7fde4b82ac4ca1e74ca1eb4ca2044d03ce506f6060003c702c0b7101ae71025b7102837102e471dd2171f28771fa13738a01738b4c738b4d7434c875837876604776e301777e0f7a424b7a426f7a42807a429a7a44177a44317a444d7a493a7a49527a49dc7cf85c7cf8767cf8777cf8947cf8f57cf91d7cf9f97cfa6c7cfa717cfad48002378002a4800338800dbd800f2fae0ef587c40487c84287c84a87cc3887cc3987cc3b87cd2287cde4881bc48840048967d2896c03896c14896c20896c23896c3189900c8a08448a08d48a290943c309ae0ddaae0e1cae60d4ae0e3cadfc70adfcefadfd02adfdb9adfde0adfe47adfe5dadfe66adfe93adfebeadff04adff4cadff60adff9aadffaeadffc3adffc5adffd8ae0018ae002eae005cae009aae0151ae0157ae01d4ae01d6ae0225ae022cae0241ae0268ae02c9ae02dbae02ebae02faae0377ae0388ae0393ae03e1ae0412ae0429ae042dae044aae047cae04b4ae04b9ae04c8ae04d1ae051dae0567ae0575ae0596ae05a3ae05e5ae05ecae061fae06e1ae07b9ae07beae07c4ae07e8ae0812ae0814ae08b0ae08e6ae0998ae099aae099cae0a23ae0a70ae0aa9ae0b0bae0b21ae0b88ae0bb2ae0bfdae0c03ae0c15ae0cb3ae0dd8ae10eaae10ebae10f0ae10feae117cae11c5ae11cfae11d1ae11d9ae1200ae123aae13ccae1438ae1447ae146aae146dae14b1ae14b2ae14b6ae1537ae16f2ae170aae1737ae173bae1748ae1796ae17bdae1868ae18d6ae1916ae1c2aae1c33ae1e97ae1e9eae1eacae1ebaae1f40ae1f52ae1f64ae1f7bae1f88ae1fd7ae1fedae2037ae2042ae20b7ae20baae20d0ae21f3ae220eae2600ae265dae26a0ae26cdae2756ae2768ae2f11ae2fa1ae478dae47abae47b1ae4828ae4836ae4a2bae4ae1ae4afdae4b52ae4be3ae4d58ae4e0eae4e96ae4eb9ae4ee7ae4eeeae4f6cae4f85ae4fbeae5003ae5122ae5153ae5242ae5256ae5260ae52cbae52cfae52d7ae52ebae531cae535eae5397ae53c3ae54d1ae569cae56a4ae56a9ae56daae571dae58a3ae58d9ae595bae5993ae59e6ae5a2aae5a3bae5a4fae5a62ae5a69ae5a73ae5aa6ae5ab3ae5adcae5b43ae5b6dae5bceae5c5fae5c95ae5d1aae5d35ae5d60ae5d81ae5d83ae5d87ae5d8cae5dbeae5dd2ae5e06ae5e07ae5e54ae5eadae5ed2ae5f0aae5f52ae6038ae603cae61cfae61f9ae6202ae620cae6225ae624eae6338ae6339ae6357ae63caae63e2ae641aae6472ae6476ae648cae64bdae6605ae695aae6a91ae6b99ae6bfcae6c01ae6c43ae6c5bae6c5eae6c80ae6c86ae6cb8ae6d9cae6dc4ae73dfae73ecae744fae7487ae77d7ae77f2af4dd8c87f01c87f05e200f0e40168e49376e847f60200c902b26203e326062f0506a20f06a2670a40210a40220a403e0a40600ac0f50ac3da0ac7dd0ac93b0aca140ba0000c4050152bb40d08790d0958142f9a14681114f11c15287d152b3c1540751553711b6c0330012632000932003932005f33fc9f33fd8133fdb333fdff33fe0f33fe7233fe9533ff1533ff4033ff4d33ffe434245534308934338a3453053474d73532c83532cd3aaaac3aaac73aaac83aaafb3aab943b76383b76993b7b393b9bb53e953a3ea0483eb57b3ebe1243c2713f90bc3f9e9b3fa93d3fbbc543c2b240535843c07443c16143c18643c20a43c23743c49843c4c743c4dd43c5da43c5e643c61b43c6dd43c76843c78543c79a43c7ab43c88743c8cb43c8e443c8ec43c90c43c91843c92143c93d43c953447d15447d4244c1e744f0e044f12544f14944f67d44f6a64678a5473c0e47819e480431480442480c23480c25480c4748104548d82248d82648d88b48f3a67a4230497c07497c23497cd1497cd64a35aa4a81844b7f794b7f9e4b7fca4b7fee4b82ae4b82af4ca3354d03c24d03c44d03ca503fd3506f3f7062607101317101357101a771020a71030b71038871f00e738a4b7434c47435887435977502a675050b76398776e7297a42567a425b7a425f7a426e7a431c7a48de7bba297cf8337cf87e7cf8887cf9e17cf9ec7cf9fc7cfa127cfa727cfaa77cfaac7cfad37cfad58002b38002e980030b800321800e1280152187c82387c84687c84787c84b87cc2087cc2587cd1987cd24880db688140f881bb5881be8881c06884019896c27896c3f8a06e98a09438a0961a1926aaaaef7ae74d4ae11c4adfc80adfc8eadfc95adfcd4adfce4adfd06adfd0aadfeb5adff0dadff41adff51adff81adff9cadffb6adffbfadffcfae0014ae0023ae0049ae006cae006eae0096ae00a6ae00b2ae00ccae013bae013dae01b9ae021dae0251ae0258ae030aae03a0ae03beae03c9ae03d2ae03dcae03ffae0416ae042eae0443ae0463ae0475ae0476ae0481ae049dae04bdae0561ae0579ae0587ae058eae060cae0631ae07cfae08fdae09a3ae09e8ae0a08ae0a20ae0a2eae0a84ae0a90ae0aacae0ab8ae0b16ae0b59ae0b96ae0babae0c3cae0c61ae0c69ae0cb7ae0ccfae0d20ae0d4bae0e0fae0e81ae0eddae10c2ae10dfae1104ae1124ae1125ae1161ae11deae11edae1256ae1279ae127aae127eae129aae12bdae1386ae139cae13c8ae13f6ae13feae1422ae1440ae146bae14cfae1579ae1589ae16e9ae1719ae174fae17afae17b3ae1842ae186fae1899ae189eae18b6ae1928ae1935ae1987ae1ab7ae1bf2ae1c23ae1c2fae1d95ae1dbbae1e7eae1e8cae1ed2ae1ed7ae1f0cae1f1cae1f29ae1f2dae1f67ae1f7dae1fc7ae1fdbae1feeae1ff7ae202bae2045ae204fae2076ae220fae222cae24efae2659ae2669ae269eae26a4ae26a6ae26d1ae26e3ae29d6ae29d7ae2bf0ae2bf2ae2bfeae2ed3ae2eecae2f99ae4499ae4795ae4865ae49e9ae4b67ae4b6bae4bb7ae4bdaae4ec3ae4ed6ae4f32ae4f41ae4f4bae50bcae50c9ae50daae50e7ae51e0ae5294ae52c6ae52e9ae5364ae536fae53a0ae54c8ae54d3ae5654ae5656ae5689ae5691ae5697ae56cbae5749ae57aeae57bfae5891ae5898ae58a1ae58f9ae593eae5975ae59ebae5a00ae5a0cae5a22ae5a2bae5a83ae5a87ae5b46ae5abcae5ae1ae5b7cae5bafae5bc9ae5bd1ae5bd9ae5bdcae5c9eae5ca5ae5cbcae5cbdae5cf0ae5d41ae5d42ae5d8eae5e83ae5e8fae5eb6ae5ecdae5eeeae5ef0ae5ef4ae5efaae5f43ae5fabae601dae603dae60cdae60efae61b9ae61d0ae61e6ae6245ae632cae638eae6409ae64bcae6602ae6909ae6913ae6a2aae6a37ae6a4dae6a72ae6a90ae6b81ae6b91ae6b9cae6c11ae6c7bae6db9ae6dbdae6e74ae6e79ae73dbae748bae74dbae77beae77cdaedab9aedb4eaf0a4aaf8023afaba9c2b0cbc87f13c87f21c87f3ee200abe400e0e40106e41aa2e485e7e48de1e49060e49927e49931e49948e8065ce80672e8c23701007801008a0200c60200fe06a27f0ac7500ac7dc0ac82c0ac82d0ac9380d077f0d0bf5142baa142d9414a11614f12515009115009a152b5e15406c154c32154ed015521f32000332001032002332004932006033fcea33fcec33fd4133fd5e33fd6b33fd9033fda633fdc733fe0933fe3233fe7733ffba3456073535423563423571d53571d63aaaa63aaaea3aaaf53aab103aab2e3aab703aaba53b75aa3b76713b76a23b77b63b7b3a3b7b3c3b7b3e3b7b6d3e97eb3e9bf73ea34a3f4efe3f6e1f3f8fed3f9c813fa6573fb65b3fbe34400eee43c17143c1f143c23943c2ad43c47a43c4f443c55743c61043c61243c68c43c79543c79b43c7a943c7dd43c88643c91743c94443caf043cf31447d01447d1044f10344f63844f65444f66c44f67144f685457c08457c0c4678a9477ff1477ff448044948047e4804b748050f480869480c4648104848105248d84748d8524984344984864a35d24aecbb4b7f834b7f854b7f924b7f934b7fc84b7fd24b82aa4ca1ed501fb9502fa7505f88506f4050ff4070625c70c07970c0d67102097103137103857103a971fa03738a4d74358c7435987583797586297587377610d878022478059e78c84d7a42627a426a7a438c7a444e7a483a7a49237a493f7b0b317cbc677cf84f7cf8667cf88c7cf88f7cf8f37cf9b27cf9d27cfa2f7cfa4980021f80023180026b8002d98002db8002f28002f680033080079d800e3080147487c41b87c84387cc3787cd21880405880416881b64881b65881b69881bb28960268960be896c08896c32896c3a896c44896c4a8a01008a07158a10108a5182a1b798ae0c0932007aadfc61adfc78adfc8badfc90adfcc3adfcc9adfcfbadfd03adfd05adfd10adfd6cadfe48adfe62adfe70adfe90adfed2adfedbadff44adff47adff82adff94ae001fae0070ae0082ae0087ae0098ae00cfae00dcae00ecae0154ae0158ae01dbae0215ae022bae022fae02ccae0304ae0321ae0323ae0326ae032dae03cbae03d8ae03daae03f1ae042cae0465ae0594ae05beae0632ae065dae0683ae06deae07d3ae0808ae0878ae0896ae08b9ae08e7ae09daae09faae0a0eae0a40ae0a45ae0ab3ae0af8ae0ba7ae0bb0ae0bd2ae0bfaae0c3eae0c55ae0c5fae0c66ae0cb6ae0dd5ae0e09ae0e47ae0e80ae0eb5ae0ee5ae10e0ae10e8ae1102ae1117ae113fae114aae114bae1156ae1176ae117dae1192ae11b2ae11b4ae11e633fd3fae127fae129eae1392ae139dae13a3ae13ecae144cae14a3ae14c4ae151cae159aae1613ae1726ae172fae1830ae19c8ae19ccae19d1ae1b9cae1bedae1c18ae1cfeae1d47ae1d80ae1dadae1db4ae1ec8ae1f36ae1fbbae1fe5ae201dae2028ae2033ae20d9ae2144ae21eeae21f2ae220cae2661ae267aae2682ae26b6ae275aae2775ae278bae27fbae2817ae2be6ae2efbae2f70ae2f9bae2fa8ae340aae47dcae47f5ae4824ae4837ae4869ae4886ae49e5ae4a23ae4a27ae4b5fae4b6eae4bdcae4cabae4ceaae4cfcae4e0aae4e20ae4e9dae4ea3ae4eb1ae4f44ae4fb1ae4fbcae50b3ae50d7ae511dae51c0ae51cbae51daae51eaae523aae527eae52c9ae52d5ae536aae538fae53bdae53ffae540aae54c6ae54ddae54ebae5666ae56e3ae5728ae579cae57adae57c0ae586dae58e0ae58e1ae58ecae599eae59a1ae59a5ae59ffae5a5bae5a74ae5a98ae5aa0ae5aacae5ac4ae5acbae5b85ae5ba9ae5bcaae5c57ae5c8eae5c97ae5d44ae5d95ae5dcdae5ea4ae5eb8ae5ec0ae5eccae5ed6ae5f3dae5f79ae5faaae5fe8ae6013ae602eae6046ae61d3ae629fae632eae63b4ae63e4ae64b9ae65ffae6a1fae6abaae6bcdae6c55ae6d43ae6d84ae6dabae6dbbae73f3ae74d9ae7797ae77a1aee57aaf3c51afa187afa18cafa191c2b00dc87f14c87f33e40095e40138e49288e8068de806b8e84901e84a5ae880e9e8c2350100d20101a20201b002b26e06a21b06a25a06a26f06a29b0a40280a404a0a40560ac9000ac92d0acafc0b41250b41960b603b0c2178142c0f142e9114f121151d05152756152b06152bdd15405e15407732000132004d33fd6033fd6333fd6633fd8a33fe2133fe2f33fe7433fe9433ffb933ffbd33ffc43426963432cd3435cc3453043532c13532c23592093593c83aaaf73aab0a3aab0c3aab763aab7a3aab893aabd13aac1b3b76593b76673b77913b7b633b7b733b9bb03b9bd443c8bc3e80df3e85c43e875e3e9fd23f568c3f80863f88483f91383fa9373fac0e3fb95043c07643c17043c30d43c31643c46543c4c343c4c643c4d243c55243c5dc43c5de43c5f843c60c43c61743c6a143c6ba43c6e543c73943c76f43c77c43c7a443c7ae43c7d143c7d443c88243c8b843c8c643c8c943c8d143c8da43c8e643c91943c93f43c94043cf2f447d02447d3644c1e244f04444f18644f42444f661457c0945f42f4678964680d14682f548043548044748047948047a48049348049748050c480862480866480c4248d82948d82f48d9c5497c08497c25497c4349842849843a4a36044a82f14b7f684b7f874b7fa74b7fb44b7fba4b7fcf4b820f4ca41f4ca44b505f66506f21507fab6831f368324a7062707062717101ab7102087102a371030371035d7103a071041c71041f71d87371f90771f90a738a41738a44738a60738a65738b4874359675865a76e30876e72a7836067a42477a449c7a49d87bb1817cf8537cf87c7cf91a7cf99d7cf9cb7cf9f67cfa067cfa807cfa8580029e8002b9800dbc800e1787c40287c81887c83287cc23881b71881b7a881b88881c018853368953fc896c288990078a02d98a02db8a2904901015a92aedadfc72adfcc8adfce9adfdd0adfddfadfde1adfe94adfee0adff48adff89adffabadffdaadffddadfff0ae0020ae0034ae0037ae0088ae0094ae00a2ae00b8ae00e1ae00f1ae0112ae0155ae015aae015bae01a1ae01c3ae01ffae0255ae0360ae036dae0382ae03c0ae03e4ae03f3ae041bae041dae0427ae0454ae0464ae04b0ae058fae05a6ae05b6ae05d3ae05e7ae0656ae0660ae06e8ae07aaae07b1ae07b6ae07ebae07faae0898ae08a9ae08b1ae08b4ae0949ae098bae09a7ae09f8ae0a1bae0a7cae0a8aae0afbae0b60ae0b82ae0b91ae0bb3ae0c1bae0c56ae0c6eae0c99ae0caeae0cb4ae0cc3ae0d42ae0d47ae0d5cae0d84ae0d92ae0db7ae0df4ae0e18ae0e49ae0e99ae0eb2ae0ebfae1127ae118aae11c3ae11d8ae11ddae1253ae13a1ae13b6ae13caae13d1ae13e5ae13fdae1412ae1421ae1457ae1472ae16e6ae1721ae174dae1791ae179cae17a5ae1804ae1862ae18feae192dae1c09ae1c12ae1c20ae1c2cae1cfaae1de4ae1e6bae1e72ae1e7fae1e87ae1e9cae1ea0ae1ec4ae1eccae1f2c152baeae1f99ae1facae1fb4ae1fbfae1fc4ae1feaae1ff5ae2002ae201aae2034ae204dae20e6ae2160ae21bcae21ccae21daae2210ae24e5ae2688ae2689ae2699ae269bae26a8ae26b4ae26d2ae2776ae278cae2903ae2904ae2908ae290cae2916ae2936ae2bdcae2bedae2bf4ae2bfcae2ee5ae2ef5ae2f59ae2f64ae2f9aae2fa3ae4792ae47a3ae47deae481dae4841ae498aae4999ae4a14ae4adfae4ae3ae4b5cae4b62ae4b86ae4b8aae4b90ae4d34ae4e21ae4eabae4ed3ae4edfae4f2cae4f40ae4f90ae4fdaae50b4ae50cbae510dae511fae51a4ae51abae51b6ae51d4ae51e8ae5278ae52e3ae5301ae530dae531bae5368ae53b8ae54dfae54e1ae54e2ae55ddae5631ae5652ae56b9ae56caae56d6ae5791ae58b3152be3ae58eeae598cae59ceae5a2cae5a4aae5ad9ae5b19ae5b6cae5bc0ae5bccae5cc6ae5cefae5d34ae5d5fae5d79ae5d98ae5e4dae5e55ae5e56ae5e6aae5ed0ae5f12ae5f29ae5f48ae5f99ae600aae600dae6030ae61feae621eae6228ae6230ae6247ae6348ae637fae68b1ae6988ae6a15ae6a17ae6a47ae6a82ae6abdae6ac1ae6b84ae6b97ae6bb1ae6c65ae6c7eae6caaae6cc3ae6d42ae6d85ae6db3ae6dceae73e2ae73e6ae73fdae77d1ae77ffaf2136af4a7bafc664c2b82dc87f0ae4010de48c6fe4992ae8068fe8c2340100860101f50200b1046000058f0006439706a27006a28a06a2960ac76e0ac7e00ac8220ac8900ac9390ac93f0ac9450ba0330d087f0d089514247d142c9c142d84142f6514f9e714fc0d151cf1151d04152b7c15535a31fed732000c32004e32005b32007233fcb933fd3033fd3633fd5f33fd8233fd9433fdac33ffb634158a34261534308c3aaaa33aaac13aaae53aab5e3aab993aaba33aabbc3aabbd3aabc23aac143b76273b76303b76923b769a3b769e3b76a13b77203b77763b7f6843c7433ea7643f43a03f88cc3f9e543fa3423fa93f43c15f43c1ae43c22f43c25243c26743c26f43c3a243c4be43c4c143c4ca43c55043c55543c61943c69843c69d43c6be43c6c443c6e843c6f443c77f43c7d643c88343c90e43c94d43c96643c971447d0e447d5744f02444f10144f42344f64e44f6a744f80244f82345f4444678a046801d4680ff46815c477fd2477ff648d85448d8e148d90e497c12497ca8497cac497ccd497cd249842f49843549844f4a35eb4a360f4b7f694b7fcd4b7fe24b82ab4b82ce4b82e54ca31e501fa050800f50821350ffd8702c6e70625b70c0767102f971f01071fa0b71fa0d738a5f744a637500d27a42157a42447a42867a428e7a42a17a442b7a44607a49577a49657bb1727bd0467bd0667cf8387cf83a7cf99b7cf9e87cf9f57cfa197cfa3b7cfa4e80021080022c8002558002668002a18002af8002b480031a800326800e228a059687c41c87c82187cc0387cc1d8880048967d48a041e8a05a78a06e08a07098a08d08a0a35adfc62adfc68adfc79adfc7cadfcb2adfcb6adfcd0adfcd2adfcd9adfcf5adfd83adfdc9adfdd8adfeb8adfeb9adfedfadff91ae0024ae002cae003cae0059ae0060ae0066ae006fae009cae00bfae00c0ae00e6ae00e8ae01e2ae01e5ae01f5ae0254ae026fae0270ae02f6ae036cae03c5ae0433ae047aae048bae0499ae04a3ae04afae04b5ae04b7ae04e6ae0532ae05bdae0612ae062aae062fae065cae0671ae0693ae0696ae06dcae06e7ae0751ae075cae07a3ae07ecae07f0ae07f6ae07f9ae0802ae0811ae0891ae08b7ae08fcae094aae0957ae09fbae0a4aae0a7eae0a97ae0af2ae0b02ae0b18ae0b1bae0b1dae0b1fae0b2bae0b3bae0b7dae0b87ae0c68ae0c84ae0cc2ae0d69ae0d76ae0dceae0dd2ae0e43ae0e4cae0e69ae0e84ae0e8eae0e91ae0eacae1167ae117eae11aeae11b9ae11ccae11cdae1232ae1236ae123bae12c4ae141aae1471ae14aeae14c6ae1703ae1722ae172eae1741ae1799ae17b0ae1851ae192bae1babae1c24ae1d64ae1dd2ae1e51ae1e75ae1e7aae1ea2ae1eb6ae1ec5ae1ecaae1f59ae1f5cae1f66ae1f6eae1f7fae1f80ae1f98ae1fb6ae1fe4ae1fefae2032ae2035ae20b9ae216aae2173ae21baae21e1ae23deae24f4ae2605ae2652ae2664ae26cfae2905ae2910ae29ddae2be7ae2be8ae2be9ae2ed4ae2ee8ae2f5cae4794ae47f1ae4810ae4838ae484aae486bae486dae4873ae4884ae4a17ae4af4ae4afbae4b25ae4b29ae4b9aae4be6ae4befae4d2fae4e0fae4ea2ae4eb0ae4f15ae4f33ae4fbaae4fe4ae50b7ae50cdae50e2ae50eaae5180ae518bae519cae51aaae51d2ae51ebae5295ae5297ae52a2ae52fcae537bae5384ae5406ae5420ae54f0ae5658ae565dae5677ae5719ae5773ae5774ae57c5ae589fae58f5ae5969ae59bbae59bcae59ddae5a3aae5a52ae5a67ae5a90ae5ac2ae5b48ae5bc7ae5bdfae5be5ae5c5dae5c69ae5cbaae5cc5ae5ccaae5d16ae5d46ae5d59ae5d82ae5d8fae5dc4ae5defae5e53ae5ea5ae5eacae5efbae5f54ae603aae603fae6051ae605dae61f7ae61fcae6204ae6206ae620aae6275ae6278ae6351ae636cae63c8ae63e9ae63efae63faae648bae649cae64aaae65f4ae65fcae68adae69c4ae6a25ae6a27ae6a76ae6a83ae6a8cae6a94ae6ab7ae6badae6bf2ae6c08ae6c20ae6c26ae6c77ae6c7fae6c81ae6d60ae6da3ae6e7dae7480ae7486ae74a9ae74c4ae77d9ae7812ae9ff8aed15baef94eaf53b8afb238c2bedbe40137e40141e4015fe48ff5e4916be49928e80648e806b4e847f20100740200b4058f0106a20406a21006a24c06a26606a273084fe00ac0050ac7470ac7df0ac92f0ac9360ac9420acab30acada0b42800ba0360d0931142dde142ed81526d8152b611533c015406715406e1551df15522c32003732007833fd5433fd5933fdbf33fddc33fdfd33fe0233fe1133feab33ff1333ff1b33fffe3571c23aaaa53aaaad3aaab53aaad13aab683aac183b76073b76293b765c3b77773b7f933e95ca3ea3833eb6f83f46db3f87433f88033f9d553fa0163fad9d3fb27e400ee640642143c0f743c18443c22e43c23543c25543c25c43c28343c2b443c30643c30b43c30e43c39143c39d43c49f43c4cb43c4df43c55843c5f643c60943c69643c6c143c6c343c74643c7d043c7d543c8c243c8df43c8e743c8ed43c93143cf33447d29447d4444f0e744f6a5467899468159473c0d477fd148040348040f48047b48084c48086548d88f48da8548f3a5497c31497c82497c9849842a4984af4b7f6b4b7fac4b7fb74b821a4c5c1a4ca1ee4d2117501f9e501f9f505f70506f46506f6a5080de50ffdf702c6570c07e710310728130738a027435927502a975862075870376e3077801587a443e7a449b7a49587bca5d7be0597cf8447cf84a7cf8697cf8817cf9e37cfa037cfa057cfa0e8003a0800f31881b82881ba1896c0e896c3c8990098a041f8a06ea8a0a27a3652bae63c5adfc67adfc6badfc82adfcb8adfce3adfceaadfcebadfd0eadfd6dadfdbbadfe0eadfe88adff65adff92adff96adff97adffb4adffd4adffd6adffe8adfff4adfffaae0009ae002dae00a4ae00c2ae00eaae0103ae0110ae0114ae0141ae0144ae014aae0207ae021eae022aae0245ae0361ae0365ae0367ae0378ae0380ae0392ae03c3ae0452ae0459ae045bae04c3ae04ddae0507ae056bae056fae0578ae05c6ae05cfae05e8ae064eae07a1ae07b5ae07d6ae07fcae080eae087fae089aae08bfae0955ae095eae0976ae09d6ae0a03ae0a71ae0abbae0b0cae0b22ae0b6cae0bb7ae0c01ae0c07ae0c4eae0c6bae0cc6ae0cf3ae0d4dae0da8ae0e08ae0e6dae1111ae1122ae112dae112eae1131ae1136ae1148ae114fae1191ae11acae11d2ae11dfae1206ae1207ae120cae1211ae1252ae12adae1395ae13aaae13b3ae1411ae1424ae143dae1444ae1468ae146cae14b7ae14b9ae170dae173cae1794ae1827ae182cae1837ae184dae1854ae191cae1c0bae1c21ae1cfbae1d41ae1d53ae1d58ae1d90ae1e5dae1e95ae1ea3ae1eb1ae1ed3ae1ed4ae1f2aae1f58ae1fadae1ff2ae200fae2017ae201cae2029ae204bae211bae213eae21c5ae21c7ae21caae21cbae21fcae2207ae26cbae26ccae2778ae2bd3ae2bfdae2ef0ae2f54ae2f62ae2faaae479dae47a7ae47eeae4833ae483fae49edae4a10ae4a28ae4a8fae4ae8ae4b3dae4b6dae4b78ae4be2ae4bebae4becae4cacae4d9aae4e4fae4eadae4ec0ae4ed1ae4ed5ae4f21ae4f53ae4f55ae4fb3ae4fe9ae4ff3ae502cae50c8ae510eae5111ae5197ae51a5ae51caae51d7ae51e7ae523dae5265ae5280ae52d0ae52d9ae5314ae5318ae5319ae5386ae53d3ae5659ae566cae56a6ae56abae56c7ae573bae57a5ae57c4ae5876ae5892ae58a9ae58deae58e7ae58f0ae59b4ae5a06ae5a1dae5a6aae5a6dae5a8dae5a95ae5aa8ae5ab8ae5ac0ae5ad1ae5aeaae5b7eae5bcbae5bd3ae5bdbae5bf4ae5bfbae5c58ae5c61ae5c65ae5cdfae5d33ae5d58ae5dcbae5e0eae5e93ae5e96ae5ea3ae5eecae5f01ae5f05ae5f3bae5f49ae5fa0ae606fae60f2ae61beae6239ae6333ae63fcae65fdae6604ae695dae6a55ae6a7bae6aa4ae6c56ae6c69ae6c74ae6c78ae6c87ae6cb9ae6cd6ae6d57ae6db0ae6e7cae6ed6ae6edaae73e4ae73f0ae73f5ae746cae74acae74c9ae74d2ae77bfae77c1ae77cfae77fbae77fdae7e91c87f3ac87f3ce400a2e400f8e48f4243c7a6e49063e49949e80645e80676e8068ce8493fe88102e8cfc50101650101770101d10101d20200b30200e40200f102019704c1a80640f00680b706a21506a24a06a26e0ac3220ac3650ac88d0ac8920ac89b0ac92a0b41140b42910ba0030c204e0c20d00c404c0d06120d07da0d08720d0bf8142d8c142d9a14f11e1526fc152a261533cc15406f15407215508415537a1577661d33411d334a32003532007633fc8b33fd3533fd3933fd4433fd8333fd9133fe2533fec733fff834158134245734338f3543c53552903571c643c7a739d62c3aab6a3aabc93aac0f3aac193b761a3b779e3b77e83b7b653eb52243c7b03f70013f8b7d7cf85945f42a43c07243c0ee43c12a43c1bb43c24b43c27943c45c43c45f43c47b43c49343c4ce43c55943c55f43c5e743c5ea43c5fa43c61343c67a43c68743c6e143c75a43c79143c88443c92d43c94c43c98f43cf3243cf34447d1c447d437cf99244f14344f14444f84445f42545f43446fbea477ff2477ff3477ff747812a4781324804894804a8480884497c22497cb54a36114a81864b7f7e4b7f824b7f884b7f8d4b7fa44b7fe14b82284b82954c5c1b4ca28b4ca420505f65506e1b506e1e506f63507f9c50800c50ff9b70625e7101aa71038e7103d471bc6571be4371f00d71f882738b4b74359975872576e3117802447a42457a424c7a425a7a425d7a42887a492c7a493e7bb0e17cf8417cf9a17cfa077cfa0a7cfa137cfa7e7cfaa380023580026e80029780031b80150c87c40d87cc0a87cd1a881b6c881b70881bc5881bf5884018896c738a0329adff8eaaecf3aaf4e7adff9bae77c2adfc65adfc69adfc71adfc76adfca3adfcffadfd13adfd70adfda3adfe31adfe69adfe7dadfe98adfe99adfea7adfec1adff61adff68adffb2adffbbadffbcadffc0adffccadffdfae0003ae0011ae003fae004eae0064ae0091ae00c8ae012fae01ecae020eae0231ae02e1ae02e4ae0316ae031dae0379ae03e3ae044cae0480ae0568ae056dae05bbae05e0ae060fae0630ae064dae0698ae074eae07baae07e3ae07fdae08a6ae08abae08e0ae08e1ae0959ae0994ae09bbae09f3ae0a07ae0a14ae0a21ae0a2fae0a67ae0a72ae0ab7ae0b8fae0ba3ae0badae0be2ae0c17ae0c28ae0c41ae0cc1ae0d2fae0d6eae0d70ae0d8fae0deeae0df1ae0dfaae0e37ae0e45ae0e7fae0e8dae0eb9ae0ed5ae0efaae10e5ae1110ae1115ae111fae1126ae1178ae1180ae1195ae119cae11c7ae11d0ae11e7ae1237ae1251ae12b8ae130cae1388ae13c9ae141bae141eae142fae147aae14bdae14ceae1528ae152fae15d0ae16faae1720ae174aae1754ae178fae17a0ae17aaae18c8ae1995ae19a5ae1bfbae1c05ae1c0fae1c13ae1c36ae1d60ae1d79ae1db1ae1dd5ae1e69ae1f5dae1f68ae1fb0ae1fd0ae1fdeae201fae204aae2051ae2116ae2139ae21b9ae21e4ae265bae2691ae2692ae26a1ae26adae26c4ae2707ae27feae29d3ae2bf7ae2c3dae2edfae2f68ae2f9dae47a2ae47ebae4811ae483cae49c4ae49e7ae4af5ae4af7ae4b45ae4b4dae4b59ae4b92ae4b93ae4bb6ae4bdeae4c63ae4e08ae4e09ae4e1aae4eacae4eccae4edbae4ef3ae4f98ae4f9aae4fcdae4fe7ae4fedae4ffaae511cae51c8ae51ccae524fae5266ae526bae5276ae5285ae52a5ae5303ae536eae5398ae5399ae53a1ae54d8ae54d9ae54f2ae54faae5649ae5651ae565eae569eae56a5ae56eaae5798ae587bae5883ae58a4ae598aae599cae59b1ae59beae5a0bae5a50ae5a51ae5abaae5abdae5aceae5ad3ae5aeeae5bc6ae5bd7ae5cceae5cddae5cebae5d09ae5d32ae5d56ae5dd9ae5deeae5df1ae5e17ae5ec5ae5edfae5f4bae6009ae6032ae6040ae6049ae60afae60eeae6331ae6342ae6355ae63b3ae649dae64acae64beae68b6ae68e0ae6962ae6a22ae6a3aae6a9bae6bb6ae6bd7ae6c2dae6c6fae6c7dae6c85ae6ca8ae6cc5ae6cc6ae6d4bae6db6ae6ed8ae77c7ae77d6c2b017c2b021c87f27c87f41e2002ee47d79e8068902004106a20d06a21d06a25706a25b06a26d06a27106a28e0a405f0ac8240acafe0acb160b60360d806c142dd414b92f151d071526d6152b6a154ca315532915533b1b4f6032002b32002d32004732005a32007132007d33fd6933fe2033fe3833fe3933fe9933ff1233ff4933ffb83425cb34308a3571c738203b3aaab33aaac53aaaf33aab483aab5c3aab983aabb83b762c3b76703b778f3b7b5a3b9b1a3b9b603b9bcc3b9bf03e82ea3f40df3f4ddf3f77aa3f80323fab93400ed0400ee943c06f43c09a43c17e43c1fd43c28e43c34243c34443c38f43c39043c4d843c50043c56743c5e543c6d443c74043c75c43c76243c93043c94143c96143c99d447d0d447d0f447d1e44f04044f04344f0c844f12444f42c44f5a144f6a244f825457c1545f4334678a44678b1477f9047812d48040a48041b48049648105048d82048d96348da8048da81497c29497caa4984364984554984624a35ab4a36187cf9cc4b7f864b7fa34b82114b82194b82204b822d4b830450815f50ff3b50ffb65110d86832497062727101a971020471031271fa0a738a69738b4e7434c77436ac7586cc76030376e7317cf83f781a867a42657a42817a431b7a432d7a48f97bc0177cf8497cf86d7cf8997cf9a47cf9af7cf9fd7cfa407cfa8280021380028780029c8002ac8002dc8002e080031c80033a80078f80079e87c00387c40087c41287c41488002c88040c881c0b888001894088896c16896c4b8a00298a01348a06588a08578a0872ae56d3ae1169ae1173aa94a3ae11eaadfcbdadfccfadfcd1adfcdcadfd07adfddeadfde7adfdedadfe12adfe79adfea6adfecbadff3eadff49adffb7adffe3ae0028ae0051ae0071ae00d2ae00e5ae0171ae0175ae0195ae01c5ae01eeae0208ae024eae026eae035bae036eae03ceae03f5ae0405ae0471ae04c6ae04cfae0502ae055eae0562ae057fae0585ae05aeae062bae07ccae0890ae08a7ae08c0ae08d6ae08daae08eeae08f0ae0954ae0990ae09b6ae09b9ae09d3ae0a6eae0aa7ae0abcae0b0eae0b6aae0b7fae0b98ae0c12ae0c26ae0c38ae0cadae0d2bae0d65ae0d67ae0d7eae0d81ae0d89ae0ddeae0df7ae0df8ae0e00ae0e1eae0e20ae0e26ae0ea5ae0eaaae10b7ae10c0ae1157ae1202ae1273ae138bae13a7ae13b8ae13bfae13c6ae13f2ae141cae1420ae1448ae14a5ae14a8ae14caae1527ae1735ae1751ae17edae184bae184cae1903ae19cdae1c2eae1d75ae1dacae1dcbae1e5eae1e9fae1ed0ae1f25ae1f2bae1f2eae1f85ae1f8eae1fe3ae1febae2006ae2059ae2062ae2065ae20b8ae20c3ae213cae213dae2666ae2668ae268aae26b3ae26b5ae26beae2731ae2763ae27f2ae2c3cae2f60ae2f6aae2fa7ae484dae4998ae4a08ae4a11ae4a12ae4a1dae4a29ae4b37ae4b55ae4b8fae4bb5ae4bbbae4d4eae4d5cae4e19ae4f28ae4f82ae4fa9ae4fb4ae50c0ae50dcae50dfae50e4ae51a6ae51aeae51c6ae524cae5277ae53a3ae54d5ae5642ae5674ae5682ae56bbae56c1ae56cdae56d0ae56edae571eae571fae5741ae5779ae579bae579fae57caae5874ae5881ae588cae589eae58b7ae58e8ae5991ae5999ae59caae59d4ae59e1ae5a05ae5a49ae5a5eae5a5fae5ac3ae5b79ae5b7bae5bd6ae5be8ae5c8cae5cccae5cdeae5ceeae5d40ae5d6cae5d9aae5f02ae5f0bae5f55ae601aae602dae6031ae60d5ae60f5ae61adae61b5ae61c0ae61c3ae6238ae625dae62fcae63b5ae63c6ae647aae648eae64afae6609ae68b5ae6912ae6a21ae6a24ae6a87ae6ba3ae6ba7ae6bfeae6c62ae6c71ae6cb0ae6d51ae6d63ae6ed9ae7474ae74a8ae74b6ae74dcae77c8ae77ceaed610af848cafa82ac2b0a3c87f0cc87f12c87f31c87f42e40096e400b3e497d7e49933e49da4e84902e88101e909fb7c79a97c79aaa37718a39a9fa651e9ab333ca3a9e1a7f1d0ac71ba0acb7d0ac7720ac7730ac7a30ac3940ac7a533fff93aac063b77d24b7fd90ac3eb0ac75c0ac1aa0ac7a23aac383b753c3b75503b76da0200a80ac3920ac3d70ac7600aca033b76a43b76a53b76ca3b7b800ac3350ac7880ac79e0ac8630ac9043aac293b761048104e0ac3360ac3ea0ac75d0ac7a60ac9053aac093aac233aac303b75523b76be3b771f7103940ac75a0ac7670ac79a0ac7e40ac99c0acba73b75a93b76d20ac1640ac7790acaca0ac7bb0ac8ad0ac8c40acad63aac323b76130200cc0ac8670ac8f83b754e0ac7893542c53aac023aac0e3b76b33b76c83b76d53b77e43b7b413b9b1f0ac3380ac7a70ac7ba0ac9160201250ac74e0ac74f0ac7803542c33b75253b770d71010a3b75373b760d3b76ba0ac76f0ac7810aca790acb7c3b76a03b76c93b77e53b7b568940163aac043b75c64c80927103870ac75b0ac7740ac8270ac9443b76bb3b7b203b9b2102008e0ac3f00ac7780ac7b70ac8af0ac8bf0ac8f70ac91d0ac3780ac77a0ac8a13aac2a3b75380ac7630ac9010aca4c3b76c20ac0000ac3dc0ac74c0ac7a00ac8f90ac9130acb943b76b60ac9080ac99b0aca470aca530acb553b75233b75b60ac0390ac3430ac3d90ac7770ac9710ac9900acaa73413903b75310ac3850ac4050ac9310ac9d10acb533b76c00ac1980ac7550ac7750ac9193aac117101360ac0cb0ac7b10ac7b90ac8b00ac9150ac9490aca523b75403b76c63b76d40ac74d0ac8b80acad90acb364b839c7103930ac8530ac8f50ac92c0ac9720acafd3aac2d3b76c10900ae0ac1570ac7590ac8c53542c47103860200ac0ac9177101a80ac3a80ac9c83b7b230ac7573b76b53b7b400ac37d0ac94b0aca870acacf3aac053b75350ac35a0ac3ed0ac76b3aac033b76d90ac7700ac9243b75490ac3ec0ac9030acac73b76d80ac9140aca6e0acb543b76113b76d03b7b220200590ac7a10ac7c20ac9eb33ffc03aac0b3b75543b77d30ac3930ac3a90ac7850ac7bc0ac9923b76d60ac3d40ac94a0acb3433ffc13aac073aac313b75343b76c50ac8a00d088b3413913aac083b7556e498420ac37f0ac7bd0ac8bc0ac9183b76b73b76b83b76dd3b7b5900aec90ac7680ac91c0acb9533ffaf3b76123b76c73b9b1e7103910ac3960ac3d60aca490aca4b3b753f3b76143b9b1d0ac3370ac7b60ac8be0ac9070acadb3b76cc3b76d13aac0c3b76c33b7b213b9b220ac3340ac7b80ac8b90ac8c00ac9223b75ba3b76d73b7b583b9b290ac0280ac3dd0ac8b30ac99e0acb353b76ae0ac7490ac7540ac75f0ac7690ac9470ac9993aac0a0ac33d0ac76d0ac8650ac91b0aca4a0acb933b75333b76db0ac2040ac3950ac7710ac77f0ac8b10aca780aca883aac280ac7650ac7860ac37c0ac8c20aca300aca483b75263b753a0ac37b0ac7980ac8bd3aac2f3b75b83b7b5771039602012d0ac33c0ac7610ac76a0ac91a3b76bf3b9b200ac7960201360ac99a0ac9c63aac153b75323b76220ac8c3e880e80200ab0201300ac7760ac7a83b753d3b76c43b771e0ac9480ac3880ac7de0ac8b50ac96f0aca500ac7e70ac9230ac9fc3b75b70ac33a0ac35d0ac74a0ac7830ac7910ac8b60ac8b70ac8bb35455648c65e0ac1a30ac9700ac78a3545553b769f3b76bd0ac8c10aca4e0acb693b75e54cc468766021ae513f3b77cb3b77ca3b77c83b77cf3b77c53b77c73b77c6ae660771f4ef71f4f071f4f271f4f471f4f671f4f771f4f8c2c363c2b3ffae626fae6262ae6263ae626eae62a2ae62a3ae62a4ae62a6ae62a7ae62a8ae62a9ae62b2ae62b3ae62b73b77d83b77d73b76ad3b76acae62bb3b76ab3b77d53b76aaae78023b76a93b76a83b76a7ae62c5c2b107ae62d245f45bae5130ae4d61ae04a0c2b111c2b11bc2b125c2b12f3b76a6c2b139c2b143c2b14dc2b157ae6d83c2b161c2b16bae5919c2b17fc2b1897cfaa17cfaa07cfa9fae64cca7e84eae56f4084f00881bc97103a3155bcf51005e51002b03e01503e004c2b1754b840808cbab35951648da8f1577113b77d07a4447af4f34683037af04676831ed70c0b470c04d70c05770c0b770c0ba70c0200180190180218990087bc0063b7ba77103a73591ca3b75ed3b7530ae2930ae62c6ae3988505c063b77e7ae74263b77103b771c3b771b3b771a3b77193b77183b77173b77163b77153b77143b77133b77123b7711ae7803ae4f1aae59833b77e6ae5980ae24daae24e03b75ec0201a7ae2744ae24c63b77cd3b77cc3b77c33b77df3b77de3b77d93b77dd3b77dc3b77db3b77daae4f19ae5984ae1e103b75653b75433b75443b77c9ae62c7ae5f8d3b7ba53b77ee3b77f13b77f03b77ef3b77ed3aabfb3b77eb3b77fd3aabfc3aabfa3aabfd3aabf83aabff3aabf93aaa983b77fa3b775d3b7ba63b7baa3b7ba43b7ba33b7ba23b7ba13b7ba03b7b9f3b7b9e3b7b9d3b7b9c3b7b9b3b7b9a3b7b983b7b973b7b963b7b953b7b943b7b933b7b923b7b913b7b903b780f3b7b8e3b7b8d3b7b8c3b7b8b3b7b523b7b513b7b503b7b4b3b7b4a3b7b49ae6ca43b77f83b77f63b77f53b77f33b77f23b77ec3b75273aabfe3b77f73b773c3b773b3b77393b77373b77293b76583b772c3b772d3b772e3b772f3b77303b77323b77333b77353b773e3b7731ae586cc2b1a7c2b2e7c2b2ddc2b2d3c2b3693b9bb43b9bb3c87f1ac87f16c87f1745104cae5736c2c1f1c87f19c87f18c2b0fdae5982c2af81c2af8bc2af27c2afbdc2afb3c2af95c2c223c2b517c2b413c2b41dc2b43bc2b427c2b431c2b445c2b44fc2b459c2b463c2b46d3b77223b77233b77243b77253b77263b7728c2b373c2af4f4984bac2af59c2af63c2af6dc2b477c2b481c2b48bc2b49fc2b4a9c2b4b3c2b4bdc2b4c7c2b4dbc2b4e5c2b4efc2b50dc2b4f9c2b495c2b215c2b229c2b23dc2b251c2b355c2b3b9c2b37dc2b535ae1e16c2b549c2b553c2b1edc2b3c3c2b585c2b5714b83a3c2b5adc2b5b7c2b5c1c2b21fc2bb25c2bb2fc2bb39c2b20bc2b247c2b233c2c1fbc2b567c2b55dc2b1cfc2b1c5c2b1bbae2926ae2929c2b52bae4a04c2b35fc2bb43c2bb4dc2bb57c2bb61c2bb6bc2bb75c2bb89c2bb93c2bb9dc2bba7c2bbb1ae62cbaea045497c9fae62ccae1b4dae62d5c2c237c2b3f5c2b3ebc2afc7c2b3d7ae2927ae292ac2c313ae4f56ae62e0ae21dcae3c3cae2f47c2bffdae61d4c2c093ae61d6ae5002aed615ae5a41ae21f7ae214dae5b18ae62d4ae538cae1716ae2759ae4fcbae73fcae73faae62b5ae62b4ae626dae5d21ae61e2ae62ceae62d6ae62dbc2bce7ae62e5ae62e7ae62f4ae6316ae631aae631bae631eae5253adff0bae6bd0ae501eae63d2ae2788ae205dae4d55ae2755ae61bdae60d7ae4b9cae4f9bae68a6ae689bae0e0aae0019ae5893ae1a5dae62cfae62ddae4ef4ae1ea7ae6190ae142eae4ba1ae00c5ae292c48104cae09623b7475affd5eaedb55ae4bb2ae62f5738bebae7816ae1826ae0c3dae61c4ae62e8ae4f87ae5ba8ae6307ae275cae1b8cc2c309ae62d0ae62d8ae108dae592dae2be1ae292d45f469ae6174ae086bae4e7271f0e2ae5e25ae6d77ae6a41ae2740ae18dbae6d87ae525aae58b1ae5d2caed1d0ae5a76ae1ab5ae5385ae7450ae62d143c30fae62daae62de7cfa1f7cfa2cae4d5eae52fdae4d4bae4f9dae62caae2056ae4ddfae62d9ae2751ae5888ae5635ae612bae613eae0bb1ae62b0ae62b8ae276eaf213caef553ae2799ae62c8ae62ebae1dd7ae1844a60ba5ae57c9ae4f5eae4f70ae4d57ae631fae4d31ae6ca1ae1dcfae4fd7af8504ae276dae183fae62cdaeeb9aae52a1ae57ddae6ce9ae4d9948d962ae62a5a71706ae010fae62b6ae58aaae6261ae60e2ae273bae62f3ae5e85ae52feae277bae2fd2ae2fd1ae6317ae740aae2541ae626cae6264ae615fae524bae62b1ae6140ae4f1fae62d7aed9b8ae2748ae4f8eae412bae62beae62bcae62bdae62bfae62c0ae62c3ae62c2ae64c4ae62c4ae62d343c9bbae62dcae62e6ae62eaae6308ae6318ae6319ae09caae631cae275dae4d29adfff6af9ba83571d8ae62baaeb3b5ae61e7ae0cd1ae4d40ae0fc4ae62df702c0aae586eafabadae2787ae5bb7ae68a4ae2bd4ae63c9af6c56ae68a0ae4f52ae3fdfae27a0ae73feae00caae63c4ae62acae73f2ae00eb71f26dae097aae1dc6ae4f60ae73f7ae5637a74e67ae1e3471ff72ae5638a9add7ae2925ae1b5bae0e65ae0ceeae74ebae74e5ae7451ae744eae744bae7411ae7408ae7409ae7407ae7406ae7405ae7404ae7403ae7401ae7400ae73ffae73fbae73f9ae73f8ae73f6ae73f4ae73e1ae73e0ae73d9ae6db7e80685ae5632ae6c42ae6d8bae6d8cae6d8dae6d8fae6d9dae6d55ae6d52ae6d4fae6d49ae6d48ae6d47ae6d46ae6d45ae6d44ae6cf8ae6cf7ae6cf6ae6cf5ae6cf4ae6c4bae6c49ae6c40ae6c3fae6c3eae6c3dae6c3cae6c3aae6c39ae6c09ae6c07ae6d892e1719ae6c05ae6c00ae6bfdae6bfaae6bfbae6bf8ae6bf6ae6bf4ae6bf0ae6bf1ae6bedae6bd8ae6bb3ae6bb4ae6abbafc663aed61aae189daef953ae5633e49218ae6a4eae6a4cae6a48ae6a49ae6a4aae6a4bae6a50ae6a51ae6a53ae6a52ae6a54ae690bae68a5ae68acae68abae68aaae68a9ae68a8ae68a7ae68a3ae68a2ae68a1ae689fae689eae689dae689cae689aae5884ae5dd8ae62f7ae2928ae58beae5286ae60e4ae4b97ae277cae6050c2c31dae4d3d7cfa1dae4f387585d98a08408a08418a083fae779fae16f8ae5d22ae4f84ae63cbae1ab6ae1def3593c93b76cf8a02daae5a09ae4797ae479fae47afae1ddcae0161ae5977ae5d29ae64c0ae4d41ae68f2ae6156ae4d42ae4d43ae4d44ae4d45ae4d46ae4d47ae4d4a758625ae4f5bae6260ae629eae6294ae1436ae61c8ae6159507fa8ae4985ae591d43c45943c40daedb51ae4d2eaea0f97cfa27ae2602aeeba1ae4d350d09a8ae5c0baebbcbaeeb9c3b75284b84183b762badffbeae7413e4008a71f104353607afcdadae6208ae6be1ae26f43b7608ae4b403aac10ae5634aec22f48da4580030dae4f3bae604ce49930ae5c56ae56363b7529ae4b4cadff7bae6322ae5630ae11f706a20bae4b26ae4b28ae4b2aae4b2bae4b2eae4b2fae4b32ae4b36ae4b39ae4b38ae4b3a4b836cae4b3c710399af84873593ceae292eae56bd43c96eae60deae5aaac2b31943c9bd7cfa1e43c9adae1aa4ae18e8ae4f453b752c4b841180030ea8b6beae747c094020ae53f0ae138caef9508a10114b8417ae4f79ae1dceae2bdaae2bdeae292f3b752aae5e88ae293180030fae604eae4f2e3b752bae09583aaabbae116643c9aa3b752dae2932ae5d19ae6279ae5d2d3b770fae4bb47cfaa23b752eae4f49ae4f4aae4f47ae4f48800310ae68f10d09990c217f4683b4ae7414adfd74adfd73adfd7f702c09ae4f46ae56f5ae520f3b752fadfd79ae4fd843c9940d540880031143c96f43c970adfd7dadfd7badfd76adfd713b755dae54bfae6289ae6280ae2933ae74523b7681ae60ddae60e3ae60e7ae60e5ae60e6ae60e1ae60e0ae60dfae60daae60dbae60d8ae60d9ae60dc3b9b04ae60ceae60cfae60d0ae60ccae60caae5e8dae60c7ae5e8bae5e89ae5e86ae5e7fae5e80ae5e81ae5e82ae5dd7ae5dd5ae5dd3ae5dd1ae5aa9ae5aa7ae5aa5ae5aa2ae5aa3ae5a9cae5a9bae5a9aae5a99ae5727ae53f5ae53f8ae4e70ae4e71ae5929ae29343b7697ae13e8ae0048ae6b6dae53f1aee372ae5978ae4fffae5000ae5001ae4ff9ae4fcaae4fc8ae4fa2ae4f8dae4f89ae4f86ae4f7cae4f7dae4f7eae4f7aae4b5eadff7375862baf38c4359403ae5979af3bbc8a042243c9b83aab3fae4b87ae7415ae74163b775cae597a7103177103533b77feae597b8016f8ae74423b77ff4b8415884802ae61e3ae61e1ae61e4ae62908016f9ae2777a99660a7fdeca8055aa99a17a34723a0f56ba0f922a0fcd9a10090a10447a2b320a2ec0cae6b10a9e9f7a67024abcac9abf0fdaa7e20a66b3ba8b2fca033a9a0979da09aacae034dad3226a4b786a20bae4678afaabad3a0a530a0b34fa1fa1ea2a399a33d5ca3c6663b9bf23533c3a57edfa5e6c8a628caa62cc7a630e7a68fb2a6d883a6da81a6dff4a71981a71b04a71b1da74181a7855ba966b1a966b2aa690eaa7e89aa7eacaa7ecfaa7ef2aaaa07aaaa2aaaacecaaad32aaad55aab0e9aaad78aab198aabab0aabba54b8392a7a451a52994a69a4bae6500a39981a412baab17ddac544aac5801acc1c5acfbb7ada4d5adc44faddd7a87cd2fae5cf6ae59184b833aa87889adc21eadd70aadedbeae53884b83a74b839b4b8414ab05f200fe014b83344b833b4b83554b83474b83974b83384b834a4b83464b83494b83544b833d3591997585d8ae2757ae743bae0d27ae0d2a4b83394b833c3f4994ae2752ae2753ae2754ae275871f664ae188ba3685da4207fa89bb0a8b80aa8bbc1adce1aac96dfa9844fae77dbae5a24a1a131a1a4e8a335eea339a5a34113a34adaa3c2afa68426a63df9a641b0a64567a3ca1da6692fa66ce6a693d2a801a370c07d70c08eae5a26ae4f1dae5bb4ae0d5bae52b0aa8bbea93240a9f290a77ae7ae0106ae6272ae6273ae6274ae189bae4ddc43c9b943c9c7ae21ea8a042a8a055bae7417ae52b4ae5a25ae52b1ae52b2ae52b7ae52bdae52beae6d6dae5a27ae1939ae1b498a04214b800f7cf9997cfa25ae0c1aae0c19896c11896c18896c1b896c197cfa2b80026caf3c5bae572eae2fd08a0428ae2786ae2780ae2781ae2782ae2785ae2789ae278aae278dae278eae278fae2790ae2792ae279bae279aae279cae279dae27a1ae27a3ae0cd2ae0cd6af54c4ae69d17cfa28ae21ddae21deae21dfae0b4c35919aaf9e24ae43ddaf384c3b75e43b75caae238c3591d34b82ff4b82e63dd2ce02003402003202003602003dae6bbaae61cbae61cdae69daae6192ae618fae618dae618bae618aae6189ae6188ae6187ae6186ae6185ae6184ae61c7ae61d2ae61d5ae61d7ae61d8ae61d9ae61ddae630eae36f6ae4b68af665aae6ced0d05d70d05ae0d05af0d05d88a1012af1644ae69dcae5279ae5270ae5271ae527aae527dae5281ae529eae52a3ae52a6ae52afae52c2ae52c3ae52d3ae52d8ae52dcae52deae52e2ae52edae52eeae52f9ae52faae52fbaae4efae196b4b83aeae52ac0a80214b835f4b83ac4b83adae52394b83354b83364b83374b83aa7585da7586217586267586274b83d64b83d54b83934b83944b83954b83964b83984b83994b839a7586b7ae5241ae5246ae5249ae524aae524d7cf9937cf9943b7f6fae472f3df5c17103607cfa2a7cfa267cfa297cfa2d7cfa2e7cfa247cfa237cfa227cfa217cfa207cfa187cfa1a7cfa1caf843908cbbb0b14403543c2ae5e4b71035a710359ae5bbaae4f6dae60c6ae60c5ae60c4ae60c3ae60c2ae60c1ae60c0ae60bfae60beae60bdae60bcae60b9ae60baae60bbae60b8ae60b7ae606b71035bae5e3b710358ae606cae606dae6061ae6062ae6063ae6064ae6065ae6066ae6067ae6068ae6069aedab7ae2bd8ae2bd5ae2bd6ae2bd9ae2bdbae2bddae2bdfae2be2ae2be3ae2be4ae17cdae5bbbae4f71ae4f72ae4f73ae4f74ae4f75ae4f61ae4f62ae4f63ae4f66ae4f67ae4f6bae4f6eae4f6fae6b4d359413881c66ae7418af85ffae00ddae5e4eae5e4fae5e52ae5e58ae5e49ae5e48ae1887af77c9aed80bae5bb9ae5bb5ae5bb6ae5bb8ae5bbcae2fd5ae2fd9ae5fb1ae5fb2ae5fb3ae5fb4ae5fb5ae5fb6ae5c72ae5c75ae17c2ae1dddae1dd4ae1dd1ae1dd8ae1dd9ae1ddaae1ddfae1de1ae1de3ae19db7a4299ae6b6eae1b16ae6bb7ae1b6920103f3594047102ffae7441ae6cceae6ccfafb674e4955fae5858c2b2f1ae181fae11f9ae1168ae637775861fae6cd07585d6758718ae5a0e80021bc2bcb5c2bcd3c2bcdd48088a48088c48088e45f4664984bdae5fb7ae5fb8ae5fb9ae5fbaae5fbbae5fbcae5fbdae5fbeae5fbfae5fc0ae5fc1ae5fc2ae5fc3ae5fc4ae5fc5ae5fc6ae5fc7ae5fc8ae5fc9ae5fcaae5fcbae5fccae5fcdae5fceae5fcfae58fbae1762ae1763ae176eae1764ae1765ae1766ae1767ae1768ae5c29ae5c2bae5c34ae5c35ae5c3aae5c3cae5c47ae176f4b83a5ae502b8a095f4b83abae1b18ae77b8aab2faa3ad38a57b3502b26c3b7f67a6692ba3e624a8649ba8574180030c800312800313ae5006ae5005ae5007ae5008ae5009ae500aae500bae500cae500dae500eae500fae5010ae5011ae5012aae4f6aeb3023aabef3aabeec2bf3f3aabec3aabebae1b19ae5c07ae5c08ae5c09ae5c0aae5c03ae5c04ae5c06ae5c05ae5c0cae5c0dae5c0eae5c0f704282881c08ae16f0ae4f17ae4d3fae4d39ae4d3bae4d3c3e9808ae50bb71039a4984b8ae1b1a4984bbae6d67ae4e634984a171031e71031a43c9d88991803dd2cb3b757b710355710357af3a67af84344984b9ae64c1ae538aae5389ae538bae0a0043c9a5a6fd49ae6ab14b84864b8413ae132ba75993ae6ca3e4992ce49938ae5bbf3b9b5e3b9b5a3b9b5b43c988ae4b65ae09d0af551eaf7ffe48d9237103814984bc46781f3dd2cf447d6587cdffae23f0af2bc5ae6ab0ae2101ae741971f8eb3b7becaf3c02ae062eae08efae5a388a070aae56be71f814ae5aaeae5aafae26ecae5931ae77b9ae5754ae5753497c9e3aab1aae6ca5ae1845ae18434a35f94984beae592e3b7be8c2c269ae5f89780328ae1b1bae4e61ae91d2ae616eae2126ae2125ae4e3baf259c4984c0ae61968848048880d3ae63cd881bafae412cae5855b1e8967cfa94ae62ab4984c14984c3e4922dae2c074984ea88480770c05548d8804844bb4844acae00bb884806884808ae4e5bae4e55ae4e56881c05aeeba2af20c1881c04ae6cfeae4e57ae4e51ae4e52ae4e5fae16ebae175bae77dc884805ae740bae740dae740eae740f884809ae2773ae277faf08a2ae1c1dae50f73df5c0ae5c6c3aab6bae52f3ae5bacaee828ae6809ae0437ae0442ae0434ae0430ae6820adff6aae4b6a06a29dae6b590a4048353606ae1b51ae1b56ae1b52ae1b53ae1b54ae1b5548088548d88348088948088848088d48088faec784ae749dae6905ae6906ae6907ae6908ae00acae2391ae1b1cae63064b829bae1953af6c00ae4e48ae4e44ae11fb738bf4702c22497ce3ae7453aecb4aae6d88ae62690d0980ae4baeb1e87dae5936ae4f50ae4f51ae5202ae1c04ae53efae00beae5e78ae5e77ae6d68ae5e79ae1e00aeeb98ae7410ae4f3dae4f3eae22113b77fbaed92202d483aed12aae67f4aeeba3ae4cf2ae0cb8ae69f8afb23aae4e59ae741aaf2172ae0c8aae6783ae1dc03aaadcae6421ae5ba4ae741bae741cae5ba7ae5ba6ae5ba5ae5ba3ae5ba2ae5babae541bae77b5ae4e64b1e796ae49e3710363ae5e428014813aabed3b7521ae779933fcb43b75f03b75f43b75f7ae62e4adfec4ae58abae0bc6ae4903ae0a34a755dcae4b82ae4bfb7103a1ae498971031f3aaaddae0941ae4cebaecaa3ae520cae2bc6ae5e7aae26f2ae2765ae0c2d4b8296ae1902af1cb702d060adff03ae4f1bae0942ae743dae53ceae4e4c0d84c133fcbc68330568330606a2794b7fbf3b7bee35360b4b82cdae4e46ae626a33fcbfae54b880029b4aecbeae77cc502fadae8372ae6056ae1fdfae5fa4c87f377103657103a535360171035ee40088ae5bfeae4e5aae4e60af0df6e40089ae6d01359517ae4ecaae0e77ae741dae741e683d90c2c255c2c20fc2c219c2c22dc2c241c2c24bc2c25fc2c273c2c27dc2c287c2c291c2c29bc2c2a5c2c2af896c5a447d4fc2bf2b48d91148d91448d91548d91348d91648da86c2bfd5af3bc9ae4f22ae5b21aaf011ae2bc4ae592aae4a88ae64b2aecef7ae2f150d08770d000f0d0878ae6cebaed60dae4c0dae2bc8154e3d3aabceae58d2ae7454ae5b23ae58d1881bccae64a371f76f7080203b7520ae60530a40043474d3ae5b1aae5b1cae5b1dae5b1eae4a89ae5b20ae5b22ae5b24ae5b25ae5b29ae5b2aaf4bb4ae593aae1b4cae589ba9d660ae57b1ae4bfaae5e3fae4dd5ae532cae4daeae5343ae5b65ae5e44ae5fdfae4da2ae4de643c81b7cf929ae69be43c812ae5921e20100ae753d43c811ae5923ae5927ae592bae592cae592fae414fae412eae4980ae13e9ae0e2eadfdafae6d2fae19b8ae13eaae6da0ae070fae18eaaff733ae62aeae62adae4e3eae593cae1b50ae412aae21b3ae2170aeeba0ae1b15a10635ae1b4fae5a55ae7499ae6a6eae6813ae2bccae6d33ae69c1ae2f4d010137ae1b57ae6a6fae627eae5fe9ae628eae62673533c2ae18abe0b2d8aed55fae49dbae1b17ae627cae1b99010148ae2f1dae2f12ae2f14c2bdcdae5b58af8437af47dfae0867ae628cae1b1dae62f2ae195d4b8362ae1b1e4bd24bae69bfae4ef2ae1b9fae7420ae58b4ae6bb8ae1b1f43c957ae098dae1103ae6c210101d7aedb4c010179ae62f0ae6304ae409370c094e80677ae77b7ae77b6ae4f57600039ae11e8ae0d0fafe1a0ae27f5ae741fae59a0ae18e1ae1b0fae62afae8955ae1b0048d922e88888e88887ae4d62ae4e4a43c956ae5de3ae54c2af8482ae54c3b3e2c2ae54c4ae54c5ae59330d095dae115cae6268ae2143ae76a6ae4d513b7721ae632143c958ae9d8743c81443c816ae4f4d3aabf4a279c5ae4e04ae85fe3b7617af8420ae62faae62f9e2005bae10f1ae4f4eae279eae34bdae69c043c80643c81575837775862d76e302ae7421ae7423ae518daf3bc23536043591c98016e5507f8aae44c98016e64aec62457c04477f953b7660ae6d8aae7815ae7814ae77a571ff4248da46ae4513ae1b0248d90434165406a24b4a34e8ae4e4bae0dc1aed33487c40bae6b6c76602aae4de043c81707037ee40086702c6dae63cfae1b03aec6ac43c818aed56b0d09e8a79b150d0998ae1b0da9add6407d90ae4e4dae1b04ae1b0e4b822933fff2af875aad237935360a45f45d43c88a46815646815dae4bf843c80743c98a43c8193b75f23b75ee3b75f13b75f53b75f83b75fa3b75fb3b75fd46f80315771015771515771714fa3914fbfa14fc0414fc0515fc0c14fc0814fc0a14fc0b14fc0e14fc0f14fc10a7e839af3bdd8a04263b9b0643c81aae18d3af8b3a43c88b43c98bae5e408016ebae7804ae4f5d7586a87586a7af878bae7425af84214b8371348089738a88af848343c81c511122ae4f2bae604b43c98943c81d400edaae6283ae6302ae5c14400eb5ae6266881c10ae625eae6281ae62a1ae62ff400ea2afc662400edc43c6cf71035fae630071038214fc0c14fc110d003fae574eae743480152202016b447d39400ed3ae4e3fae6a43af9ba443c81f400eb4400ebc43c6d23f8c79783132ac6bd0ae591b8a0a2943c81343c82043c82106a29e06a23006a23106a23b738a86e483bbe200fd400eb7e40144881bcf7586b68a06598a065a497fa306a23275835cae1b01ae1b05ae1b0c8a065b3b75ef3b75f33b75f63b75f93b75fc3b75fe710409ae5c3615771215770e15770c15771315771415771615771814fa3b800e3b783162ae6a440700bbaeeb9e7103a28a067cae1b06ae1b0b447d4eae1b58ae57cfaeeba5ae00f74b836135918e447d3545f4688a067d06a2344b83a4ae5e6d06a23eae540471031902016615771914fc09ae77bdaf9904447d598a067e06a23506a23f881c09ae51713ea427ae5770ae5767ae1b07ae5769ae576fae2f35ae2f3143c95bae77bb06a236710366710318881bb7ae77bc06a23789900506a23cae576c71f5d07832037a442cae1b08ae6a45ae5e7daf0a4c15cd2806a23806a23dae45c6ae1b0aae1b5aae1b9bae1ba080150b683d00ae576d7586f5ae5e6e0ac31e06a2393b75b933fcb1ae69cfae4c1906a233ae1b0906a29fae1afa06a290ae5e6bae5e6cae1b80ae1b8aae576eae1b60ae1b6cae1b72aedaacae1b7aae1b7bb1e7ac06a23a7a442dae62edae576a800e38ae5e67b1e877e200f906a22906a22c080010ae2bc5ae5e6806a22f06a22be847fcae62fbae630a3b7f740428daae5e6606a22eae630906a22a706047ae6ceeae51f2ae2795ae4ef5ae043fae0abdad5caa06a22dae6a46aeebabae588dae521f02c540af53a4ae63d1ae62b98a0a47ac5ba3ae60f9881bd1881bb47103abafa162aee731ae62507a4298ae54c0ae05a28a0a483b7bd28006b23b7fbcae1afcae1b62ae1b6fae1b73ae1b7cae1b81ae6292ae62964682e6ae630dae630caef04739913bae627baec2f2ae20a0ae57640acb790acb770acb7887cdf4ae69ccae6176ae5992aeffc47cfa74ae509eae509daf2ec0af3378af1a94aef8b333fcb8ae1688ae597e096025ad5af4a9a196ae751f8016e7ae4f243aab8baba78cac0a69ae195eadff6dae00e2ae4c09ae2bc2ae77b2ae77b1ae77b0ae77adae77abae77acae77a83b7f8b3b7fde3aaafaae17ce43c878ae64c8ae77b4ae77afae77aaaebe960d5430702c6cae1ae2ae4ef80d0a220d6118adffb1ae77b3ae77aeae5255ae77a9ae19a9af8026ae23aea08cfcae5127a1b9fcae1ae4881bd4881bd2ae520dc2c377ae1ae5010057ae621fae7424ae7422af4fa800856b0d064f7103acae1ae6a053fdafe1be71ff7baf9e53ae6293ae6297aef94d4b8412adfe8cae1ae7ae5212ae6aacae743e505c07ae1ae180062a71031d71031baf4fa13acbb1881bd3505c08505c0971031633ffef33fff333ffe333ffe1ae58074a34ea010024896c4f4b85cbae1aff80058508001f0d064e0d064d0d06507a43f075026d48ad0748ad0633fff1ae5c7e717c9f33ffe071f4f1010230ae4d5d68324f04200a06a03806a05a4984c233fff033ffed0d087aae4caf33ffeaae4bfe0c404fae4e6c71f4f3ae20fa3593d98a0a2cae4b444810f633ffe88a0429ae6edb3b7ba871033d0ac7c833ffe78a0424ae9a98e8cfc8ae5938aeede98a041daf84de33ffe2ae4bf98a070671f4f93b75a70be2b07a43ef78c5ac71f005af8db748da96ae81bfaf4f60a24e31aee8a1c2b323ae5803ae1b8450042f407d8faf8447ae1b65ae1b764b8c2e4bd2b20641e16008018a0a3f0983a1ae61bbae540eaf4f364ba674407dfc044037a0e824a134a60900f5ae61bcae61baa3bbabae60faae540f881bd5468101710320a6d58b14fc1714fc16b1e7a1038f42a967be0386133b7b8a71f007ae6105ae1b66ae1b78ae1b85c2b201c2c36dab87c170c056038ffe038f47038f45ae6244ae60fd038f46038fffae1b67ae1b79ae1b87a9aa6bae5c44ae5c21ae5c22ae5c2dae5c2eae5c2fae5c30ae5c31ae5c32ae5c33ae5c37ae5c38ae5c39ae5c3bae5c3dae5c40ae5c48038615038f4aae5c23ae5c3eae5c41ae5c49ae5c4caae8e0ae5e647103923b7766ae77c33b77653aab8fae53da8002d8ae239fafab96adff3badff3aae5c24ae5c3fae5c42ae5c4aae5c5071f0003b9bd5af9b75ae5c53adff3dae5c25ae5c43ae5c4bae5c51896c1cae1b6871f4edae1b88ae407371f00102c5443f992bae501dae5c26e4955571f5ea71f4c971f4c171f4ee71f4cf71f65c71f4cb71f4ca71f4c771f4c271f4c371f4c571f4c671f4c471f4c871f4cc71f4cd71f4ce71f4d071f4d471f4d571f4d671f4d771f64d71f4d871f4d971f4db71f4dc71f64a71fc0571f4dd71f4de71f4df71f4e171f4e271f4e071f4e371f4e571f4e671f4e771f4e871f4e971f4ea71f4eb71f4ec71f4d171f4da71f4e4728520ae5c27ae5c45ae5c4dae5c52717cbcae60feaf653171f4d271f4f571f4fa71f40071f40171f40271f403ae5c46ae5c4eae0e82728521ae1b6aae1b89ae60fbae24eaaf6ec3ae6100ae6102ae6104ae274fae4aecae62463aab84ae5c4fafbfee71f4d3ae6bdaae2383ae6057ae62efae63d7ae1afdae1b5dae1b5eae1b5fae1b63ae1b6bae1b7eae1b82ae1b83af3f84ae5f88af05d9ae53e1ae6233ae5616ae1c03ae5b5fae6243ae5228ae5bf671f267ae6d0668324daee660aeea98ae6103adfee1ae4d28ae5e5aae5e5baf841caf4faaae470e0d0bf2ae62aaae62fdae62fe7806fdae6311ae44f7ae6313717ca6ae6412af0930ae3341afa189ae6277ae1aecaf5356ae6dcbaaec2445f462ae6edcae23ab780039ae5e617a43f1ae5706ae23acae56f6ae23aaae6606ae517bae6055ae6bcbae23adae4e69ae4e62ae4e653aab97ae4e67ae742eae568c14fc1214fc1314fc1414fc15151d41151d42ae4e66155bd0155bea155beb14f113147991148000152a8d152c29152c2d152baf152ba97324e57324e17324e37324e47324e67500d5467807ae4130ae2769ae1b31ae6291ae6295ae6301af266f46782d46782eae651e80069baf231333ffe6ae6c2370002f46784746781246781346781d46783744f0e148d882ae26eeae6000ae7428ae49ad43c81eae4f8104c1e8ae5f1bae6993084003ae5f1c084004ae5f1dae4ae4be09c0728135ae779bae03fc06804eae779c76603571f006ae6242ae6241ae1b32467808e40123ae0cb0ae570746784446781446782046782f46783844f12104c1e9ae1b3300fe05ae624f467809467843ae694aae6994ae5f1e46781546782146783087c8570703fa71f251881b798a042380152004c20e3591853593d4447d2d3b7f76447d5571f88846783a04c1eaafb23e3b76a346780a46784246781646782246783171f253447d66ae5f24ae5f1fae6790ae1b3446780c46784146781746782346783246783baece5dae604aae77f64b83b5ae77f5ae591cafc65fafb89cae9f44800314800e0c800dbe800315aff006ae591eaec91f46780d467818ae1b71ae74554678244678333d9340800e0e447d0b728523ae17c5ae1b3546783cae591f0d07d571035ce4993d477fb6881bd771f2d3ae5606ae591aae5916e9019171f003ae5915ae53edae53f9ae1b3609008846781946782646783446783d71f008ae1b3746780f884803e200dde0164546781a467827467835ae6b7dae63d0ae742aaf662d89630246783e71f00271f009ae59170a404bae4ecb0180003aaae4884009896c45ae63d4ae641446781146781b46782946783646783f45f461ae5a16ae5a10738bf5ae6410ae6415ae6416ae53b7ae53b0ae53c0ae53c2ae53a9ae1afe46780bae63d546782aae6411467840ae0f9245f463ae6417ae53b1ae53baae53c4ae53a8ae53aaae1b39ae1b64ae1b75ae1b7fae1b8bae1b9dae1ba10e1838ae53bbae53c5ae617fae53abae17f80360b2ae1b2dae1b203aaaee3aaadf3aaadb467839ae5bf7ae63d6ae6418ae5bf8ae53b20800ccae5bfcae1b25afb721ae1b2bae1b2eae1b10ae1b12af3df3ae1b21ae1b26ae1b2cae1b1171f14eae5bfaae5bfdae6419ae53b3ae53bcaa0cf5ae53c6ae53acaea9f6ae53b4881bb6adff95ae0de4ae53beae440bae53c7ae53adae0a5fae4409ae53a4ae1b59ae4f0aae640bae0f0cae1b27aeeba4ae77cbac817fac810fac738fa4d9aaae6320af6bcbae1b23aefc4dae53b5ae53bfae53c8aedaa643c5c5ae5f20ae53ae485920ae640c710002ae1b28ae1b2fae1b13ae5de0ae5de1ae2480ae24817cfb0bae2483ae53c9ae53afae640dae55a2a444d6af2a7aae5f21084017ae55a1c2b59971f09eae5e6371f14571f142ae4e4971f09dae629bae2484ae2485e483baae5930afffbeae2f45ae2f2cae2080ae159eae15e3030006ae6a57ae53cbae5f22ae53a7ae640e8a0855396084ae56aaa6d0a8ae53ccae640fae7e67ae1b29ae1b1406a3cf894082ae1b24ae6b1b02b26b02b26a89602ba71feca7478eaf8606a6f84aaf8489ae53cdae0956ae742cac9cfeaee737e404094984bf3b7faf33fc973b7771ae6285ae5f23ae53b6fedd2f71f013af50e9af564ba0ec9806a2c3717c73ae53d5ae7817ae5de7ae5de83b9b5cae62f1ae5d23ae62e3ae6303601142ae53dbae62eea9aa23afe21dae1b2aa9adf8896c42aae8d0afb234afb673ae11f4ae4e42ae5bb3b1e7d9ae53e9ae40bcafdfe2afdcd3ae77efae5befae073cae20e8ae20e9ae20f0ae20f1ae20fcae20ffae2100ae6470ae2110ae2117ae211aae211d71f112ae2120515001ae2122ae20e7ae5bebae573071f0d6ae5be9aeffd8a6b8b3ae699f3b776cae20eaae50a8adfec7018047ae20f2ae20feae2102ae2112ae2118ae2121ae20e5ae5bec71fa6caf4f43ae562faf3bde80151fae4bc2a010efae20ebaef51fae62e9ae68d3ae5c15ae2123adff27ae6be5aedaa5ae743fae7440ae23a2ae20ec601861ae20f4ae2104ae20e1ae5133e40113ae5beeae77edae20edae20f3ae2105ae20e0ae59c2c2c007ae1b7d8964c90ac7ea3aac00ae09a9506e18af3c4eae2106ae20e202016a02016735940e3b7fb47a43ed880417ae20efae2109ae211c06a0a2ae5fa6ae210aae6288ae742bae1b30ae1b8dae1b9eaf9ba9ae210bae211fae62c9ae58bcae77eeae629aae210caeda3eae5028ae077168325070604a60181a89629f09cd55ae77f0601819ae210d4713bfae65f30201653b7772ae1b3aae1b3bae1b40af76c5af3d52ae210e6018033b7564ae21033b77c400a272aeeba7ae5937ae2bc0ae0541ae6b54ae1b3cae1b41ae1b93ae1b96ae6b360d83733b77fc5002cbae4a79ae4c07c2b58f0ac8c6ae6d03af3fe93b7fd7ae1b3dae1b42ae1b94ae1b97ae36ea43caee7cfafeafea5f3b7559ae780771f171ae5986ae6bbdadff6bc2b503ae5b3171f106ae6bbcae1b3eae1b43ae1b98896572ae5191c2b409ae4f0bae693eae6248ae5beaae622aae623fae5dbbae4b99ae624a7cfafc7cfaff7cfb00ae5dbaae5db90201a671f2b7ae6171ae0a37ae5df0ae6451ae09afae248c480850ae0ba1ae0e11c032e3af85fcae4a80ae440cae19fcae840faf7645ae6177afd871480886ae51758016edae1b3fae68eaae63d3c2c09d480887ae1b45e847fdae4bafc032e5ae610aae18f970604cc032ecae6d02ae6cffae6d003b75633b7761ae49f6ae4e45ae6d07ae6d0407c001ae6d0571f004afb79f800218800e1b800208c032edae5913ae5920ae5922ae5925ae5926ae1b46ae5928ae6324ae6325c2bf71ae5813ae4c02ae4c03ae4bfcae4bfdae4c00afca17a7e796ae2f40ae5172c032eeae5924ae2f41ae6326ae7498ae6970ae18cbc032f2ae632706a00fae1b47a62a05a97166ab334349844cc032f9ae5201ae4c10ae2353afa161ae5165881bcd738a484810f870c12eaf2567ae2655ae0c4d314c65af3bceaf843ca33d33ae5174ae5176ae517fae5182ae5188ae518aae518eae5190ae5192ae5194ae6bdfaeecf1ae51664b8681ae5173ae5177ae51843b756aae5189ae518cae518fae52204b8268aed554aa0fffae5ddd7cfa97afa55371f020ae5167af0811ae5178ae518571f2e6ae5224ae522baeeaf0aeeba6ae5932ae4e6aae4e6dae4e6e45f467ae6b82ae5e7bae4e3a7cfafdae4e4eae0dd6ae0eeeaf1dccaf1c26ae6ce2ae5f7fae5f7dae5ddbae4e6ba5c52dae512aae5f7ea88e57aaaa7006a01c7cfa93af20a2af1cdbadb8c0ae5168ae5179a1f5f1ae5186ae5ddcae5169ae517aae1b48af83a7afa16ce489baadfdabae5ddec0532cae516aae516bae4e5cae5ddfadff35ae4e53ba4e53e4014087cc05ae1a26ae516c507c4f896c53ae5939ae5934ae9d45aea33caf9a2eae6cecae516dae1c014b9c300e07183b7760ae6386ae516eafa8223aab96a88e473b7763ae4e5d46f593aaecfbc041fec02044c041f5c041fcc041fbae5f84aae7fd485694c041fac0204bc0204cc0204ac02043ae2bcac041f7c02047c032f3ae64deafe89aaf973d7101e203001271010bc041f6c020457101e9b1e50c45f464affa2dae0b17ae5974ae5976aec97e71f0fac041f3af54bbc2b57b894081aeb7f27101953b7769ae8c2402016804200306430c71f01fc2b5a3348088881c684b80173b776eae6a597101f0ae24f3ae24f5ae5e62ae24fc3b7b89afc735afead17a433ac2bf8fae24f6084002af6beeaf3bcfaf78fda3ed57e400d9aa228d738a89881bea7a43eeafb22cae6d6cafa7e9ae24f9ae24fbaf4f46738be8ae6286afdbcc710362ae24faae24fdae6287afa80fafef5fafdaa4896c75ae563aafd81eafe62fae4e50ae4e5eae5886ae4e54ae0778ae50deafb66c71f0dfafdfacae2115ae09b8afa5d371151d71f10caf85c6e49532ae8915ad7069ae4d4dae7b14ae851eae5f87ae5f864aec63ae2111ae2113ae6bbba9ad72aaec6e738bf2ab5ab9ae616dae610cae78054856f3ae7806717cde3b776bae7fffae2399ae2398ae5ba0ae637cae6bc871019bae5bf9aee81fae88a8af3bc6ae6153ae6b1faf987bae22c6ae6183ae617dae617bae617aae614bae6147ae6173ae6175ae6168ae6163ae6155ae613cae613bae613aae5765ae5204e20026afca98afde2cafbab843c575e20027e20025ae61800d81ecb9c2cfae2bc7062f2eaf9e29ae6114ae6113adff85aed556ae2658ae33f73b7620aed558ae53eee2001f6830ed08a081ae6167ae5de9ae1484ae274ec02829ae50feaf9fdfaebb0b1e3c8ac032fac02835c0282cae53d9ae53d7ae5f81ae5f8aae5f8bae56f3c06ccbae6a58ae6a56ae53eba1e9530100d1af9b683b7770c032e4018098ae585dae26f6ae2f2eae92bd3b7b88728522ae4b70ae5211ae4c0ac0282bae53d6ae5f82ae099dae5f8cc02825c02833ae58d73b7b87ae58d4ae58daae58dbc02834c032f8ae5f83ae5170ae517c01008fae517dae58d5ae58dcae58d6ae58ddae1904afc669af6063ae6d6eae58d8af382188436f87c851c2be9587c8523f9640ae16cb87c84fae5e7c87c853ae57dcae3549ae276671ffadae74a0a3293e87c85071ffaf87c854459901ae2f21ae2f20ae2f22ae2f16ae2f1bae2f1cae2f0fae2f0aae2f0cae43e787c855ae53e6ae6dc7ae53e2ae53e887c819ae0c51c2b53fae6dc80ae044ae6d09ae2f173b7f66aed90c87c856ae53e3ae2f1e87c81cae2f0daf85b8af3beaafb21bae2f1fae2f0e87c85887c81dae2f0387c85987c81f3b7764ae53e4ae53ea3b776287c82087c82787c82a87c82c87c82d87c82f87c8304243fd87c8338960b4459902ae53e56830a53b776daf3294ae53e7ae04efc2af77aeac73ae7de7497cd5ae53f4ae53f6ae5e60ae53f74b83684b83644b8365c2af9fae0cceae23afae23a3c02828ae6d6ac032f4c0282aaed0950acc67ae4b7c497fa487cdf3afca20ae23a4c02820afb9daae5f90a68b92ae01d371032cae586aae6d08ae23a5af486aae23a6a60693aee4e4ae23a73b7767ae18fcae59144b8b32ae6d69ae6cefae4bc0ae4bc1ae4bc3af532a7cfb0c89636eae23a8ae2f34ae4bc4ae23a9ae4bc57586faab009dae50f0aaf443ae4bc6ae6b23ae77f1ae0d3fa0299ea1bbe571f01aaada4cae086c71f01bae00efaafaaaae0862ae0864ae0870adff20ae4e3cae0400ae10e4ae291fae60f8ae7433af080eae0cfcae4bc7ae0866ae086dae0871adff19ae4e3dae6dccae6b8dae53deae53dfae6314ae631543c6a9aed8afae49d7ae086eae0872adff1eaf6379706049ae4bc8ae0873ae4bd8af0fd0ae4bc9aff00eae53f33b7768ae0876ae53dd4b90300900b5ae53dc717ca706a0013b775faf3bbeae2739ae5f4aae5669c032efc2c2ebae1bfeaf476eae0c30c041efc0282dc06ce0c082f060182589404d3b77d43b776aa73d39ae40717cfaa8ae4c0cae4c0baefb00ae55c9ae55ccae5526ae55ffae5704ae572dae572aae572fae6a00ae6d70ae6d6f042088ae4e8bae2387ae69c6ae69d0ae5705ae572bae4e8c71154aaf63f57101e3717c7989407faab59571f2a1ae2bd0c2be27ae169faf9b733b9b19ae66083b77ac0201c80201c9ae74560200ba0c405c0c405aaf7f780200bd0200c002018e0200c30200c20201a90201aa02014dae77f4af0a53afca1f02018f020192020194020161020181ae7431ae23640b4354af0db60b4150af4f2faef1c78962647060243b775e600be9ae69d2a808e7a7f654a983a3a97feca97c35a81055a30beca23027ae53f2aff785af6164ae08c30201900201930200be0201ad02014f020195ae1985a80c9e3b775ba7fa0b717cb53b76b0896636710334a7f29d0201910201ae02015eaaf024ae52374b8410020074ae4c01ae5935ae593bae61540c405b600bc0894049a022e54b8c4b600be8600bc43b7522a2fad10201520201534678ac3534480d082c07037dae5b9dc053257586f6881762881764881766800f21800f2b800f25800f24800f2a800f13800f10800e3d80028280028e80028880028980028a800280800283800dc4800dc280148280148380148a800e648007a08007a2507f9e4b838280034a80035080035a80075880078880075a80075b400ebd7586f47586f7881761881763881765881767800f28800f2d800f1480028480028102014ca7a2be3b7566800285801484800e658007a180034980034b80035180035b80075980078980028d80028b80075c80078a80075da43bea800f29800f15800286af486f801485800e6680034c80035280035c801486800e6780034d80035380035d80078b80075e60183e71022bc2b5cb0c21d0e4a32e801487800e6880034e80035480078c80075fa56a0060185a8a042580148880035580078d3b75694246a970604b7101905003eb3b75dd80034f80035671152a48448c6008078003578940148003588960b1ae182d8a0705aff79eae182faff005ae1aafaf6b67ae198eae7808af65cc800359aaf34cae182eae1ab0ae1ab2ae1ab371f0f7afb8a2ae6cf0ae4a01711516896571aed0d7ae4c084b83b04b83a6ae630bae630fe200fe71f027ae6cb2a7c0f23b762106a0213b7567ae1ab1ae1ab43b776f3b75a802000f3b75453b756b3b75420420015160018940c07101e73b7568502fab502fac4b8331507c537103334246b14599034599048940c8a21be11d3340";
var MILITARY_HEX_SET = new Set(
  Array.from({ length: _HEX_PACKED.length / 6 }, (_, i) => _HEX_PACKED.slice(i * 6, i * 6 + 6))
);
function isMilitaryHex(hexId) {
  if (!hexId) return false;
  return MILITARY_HEX_SET.has(String(hexId).replace(/^~/, "").toLowerCase());
}
var MILITARY_PREFIXES = [
  "RCH",
  "REACH",
  "MOOSE",
  "EVAC",
  "DUSTOFF",
  "PEDRO",
  "DUKE",
  "HAVOC",
  "KNIFE",
  "WARHAWK",
  "VIPER",
  "RAGE",
  "FURY",
  "SHELL",
  "TEXACO",
  "ARCO",
  "ESSO",
  "PETRO",
  "SENTRY",
  "AWACS",
  "MAGIC",
  "DISCO",
  "DARKSTAR",
  "COBRA",
  "PYTHON",
  "RAPTOR",
  "EAGLE",
  "HAWK",
  "TALON",
  "BOXER",
  "OMNI",
  "TOPCAT",
  "SKULL",
  "REAPER",
  "HUNTER",
  "ARMY",
  "NAVY",
  "USAF",
  "USMC",
  "USCG",
  "CNV",
  "EXEC",
  "NATO",
  "GAF",
  "RRF",
  "RAF",
  "FAF",
  "IAF",
  "RNLAF",
  "BAF",
  "DAF",
  "HAF",
  "PAF",
  "SWORD",
  "LANCE",
  "ARROW",
  "SPARTAN",
  "RSAF",
  "EMIRI",
  "UAEAF",
  "KAF",
  "QAF",
  "BAHAF",
  "OMAAF",
  "IRIAF",
  "IRGC",
  "TUAF",
  "RSD",
  "RFF",
  "VKS",
  "CHN",
  "PLAAF",
  "PLA"
];
var SHORT_MILITARY_PREFIXES = ["AE", "RF", "TF", "PAT", "SAM", "OPS", "CTF", "IRG", "TAF"];
var AIRLINE_CODES = /* @__PURE__ */ new Set([
  "SVA",
  "QTR",
  "THY",
  "UAE",
  "ETD",
  "GFA",
  "MEA",
  "RJA",
  "KAC",
  "ELY",
  "IAW",
  "IRA",
  "MSR",
  "SYR",
  "PGT",
  "AXB",
  "FDB",
  "KNE",
  "FAD",
  "ADY",
  "OMA",
  "ABQ",
  "ABY",
  "NIA",
  "FJA",
  "SWR",
  "HZA",
  "OMS",
  "EGF",
  "NOS",
  "SXD",
  "BAW",
  "AFR",
  "DLH",
  "KLM",
  "AUA",
  "SAS",
  "FIN",
  "LOT",
  "AZA",
  "TAP",
  "IBE",
  "VLG",
  "RYR",
  "EZY",
  "WZZ",
  "NOZ",
  "BEL",
  "AEE",
  "ROT",
  "AIC",
  "CPA",
  "SIA",
  "MAS",
  "THA",
  "VNM",
  "JAL",
  "ANA",
  "KAL",
  "AAR",
  "EVA",
  "CAL",
  "CCA",
  "CES",
  "CSN",
  "HDA",
  "CHH",
  "CXA",
  "GIA",
  "PAL",
  "SLK",
  "AAL",
  "DAL",
  "UAL",
  "SWA",
  "JBU",
  "FFT",
  "ASA",
  "NKS",
  "WJA",
  "ACA",
  "FDX",
  "UPS",
  "GTI",
  "ABW",
  "CLX",
  "MPH",
  "AIR",
  "SKY",
  "JET"
]);
function isMilitaryCallsign(callsign) {
  if (!callsign) return false;
  const cs = callsign.toUpperCase().trim();
  for (const prefix of MILITARY_PREFIXES) {
    if (cs.startsWith(prefix)) return true;
  }
  for (const prefix of SHORT_MILITARY_PREFIXES) {
    if (cs.startsWith(prefix) && cs.length > prefix.length && /\d/.test(cs.charAt(prefix.length))) return true;
  }
  if (/^[A-Z]{3}\d{1,2}$/.test(cs)) {
    const prefix = cs.slice(0, 3);
    if (!AIRLINE_CODES.has(prefix)) return true;
  }
  return false;
}
function detectAircraftType(callsign) {
  if (!callsign) return "unknown";
  const cs = callsign.toUpperCase().trim();
  if (/^(SHELL|TEXACO|ARCO|ESSO|PETRO|KC|STRAT)/.test(cs)) return "tanker";
  if (/^(SENTRY|AWACS|MAGIC|DISCO|DARKSTAR|E3|E8|E6)/.test(cs)) return "awacs";
  if (/^(RCH|REACH|MOOSE|EVAC|DUSTOFF|C17|C5|C130|C40)/.test(cs)) return "transport";
  if (/^(HOMER|OLIVE|JAKE|PSEUDO|GORDO|RC|U2|SR)/.test(cs)) return "reconnaissance";
  if (/^(RQ|MQ|REAPER|PREDATOR|GLOBAL)/.test(cs)) return "drone";
  if (/^(DEATH|BONE|DOOM|B52|B1|B2)/.test(cs)) return "bomber";
  return "unknown";
}
var POSTURE_THEATERS = [
  { id: "iran-theater", name: "Iran Theater", bounds: { north: 42, south: 20, east: 65, west: 30 }, thresholds: { elevated: 8, critical: 20 }, strikeIndicators: { minTankers: 2, minAwacs: 1, minFighters: 5 } },
  { id: "taiwan-theater", name: "Taiwan Strait", bounds: { north: 30, south: 18, east: 130, west: 115 }, thresholds: { elevated: 6, critical: 15 }, strikeIndicators: { minTankers: 1, minAwacs: 1, minFighters: 4 } },
  { id: "baltic-theater", name: "Baltic Theater", bounds: { north: 65, south: 52, east: 32, west: 10 }, thresholds: { elevated: 5, critical: 12 }, strikeIndicators: { minTankers: 1, minAwacs: 1, minFighters: 3 } },
  { id: "blacksea-theater", name: "Black Sea", bounds: { north: 48, south: 40, east: 42, west: 26 }, thresholds: { elevated: 4, critical: 10 }, strikeIndicators: { minTankers: 1, minAwacs: 1, minFighters: 3 } },
  { id: "korea-theater", name: "Korean Peninsula", bounds: { north: 43, south: 33, east: 132, west: 124 }, thresholds: { elevated: 5, critical: 12 }, strikeIndicators: { minTankers: 1, minAwacs: 1, minFighters: 3 } },
  { id: "south-china-sea", name: "South China Sea", bounds: { north: 25, south: 5, east: 121, west: 105 }, thresholds: { elevated: 6, critical: 15 }, strikeIndicators: { minTankers: 1, minAwacs: 1, minFighters: 4 } },
  { id: "east-med-theater", name: "Eastern Mediterranean", bounds: { north: 37, south: 33, east: 37, west: 25 }, thresholds: { elevated: 4, critical: 10 }, strikeIndicators: { minTankers: 1, minAwacs: 1, minFighters: 3 } },
  { id: "israel-gaza-theater", name: "Israel/Gaza", bounds: { north: 33, south: 29, east: 36, west: 33 }, thresholds: { elevated: 3, critical: 8 }, strikeIndicators: { minTankers: 1, minAwacs: 1, minFighters: 3 } },
  { id: "yemen-redsea-theater", name: "Yemen/Red Sea", bounds: { north: 22, south: 11, east: 54, west: 32 }, thresholds: { elevated: 4, critical: 10 }, strikeIndicators: { minTankers: 1, minAwacs: 1, minFighters: 3 } }
];
var UPSTREAM_TIMEOUT_MS5 = 2e4;
function mapWingbitsDetails(icao24, data) {
  return {
    icao24,
    registration: String(data.registration ?? ""),
    manufacturerIcao: String(data.manufacturerIcao ?? ""),
    manufacturerName: String(data.manufacturerName ?? ""),
    model: String(data.model ?? ""),
    typecode: String(data.typecode ?? ""),
    serialNumber: String(data.serialNumber ?? ""),
    icaoAircraftType: String(data.icaoAircraftType ?? ""),
    operator: String(data.operator ?? ""),
    operatorCallsign: String(data.operatorCallsign ?? ""),
    operatorIcao: String(data.operatorIcao ?? ""),
    owner: String(data.owner ?? ""),
    built: String(data.built ?? ""),
    engines: String(data.engines ?? ""),
    categoryDescription: String(data.categoryDescription ?? "")
  };
}

// server/worldmonitor/military/v1/list-military-flights.ts
var REDIS_CACHE_KEY32 = "military:flights:v1";
var REDIS_CACHE_TTL32 = 600;
var quantize = (v, step) => Math.round(v / step) * step;
var BBOX_GRID_STEP = 1;
function getRelayRequestHeaders2() {
  const headers = {
    Accept: "application/json",
    "User-Agent": CHROME_UA
  };
  const relaySecret = process.env.RELAY_SHARED_SECRET;
  if (relaySecret) {
    const relayHeader = (process.env.RELAY_AUTH_HEADER || "x-relay-key").toLowerCase();
    headers[relayHeader] = relaySecret;
    headers.Authorization = `Bearer ${relaySecret}`;
  }
  return headers;
}
function normalizeBounds(req) {
  return {
    south: Math.min(req.swLat, req.neLat),
    north: Math.max(req.swLat, req.neLat),
    west: Math.min(req.swLon, req.neLon),
    east: Math.max(req.swLon, req.neLon)
  };
}
function filterFlightsToBounds(flights, bounds) {
  return flights.filter((flight) => {
    const lat = flight.location?.latitude;
    const lon = flight.location?.longitude;
    if (lat == null || lon == null) return false;
    return lat >= bounds.south && lat <= bounds.north && lon >= bounds.west && lon <= bounds.east;
  });
}
var AIRCRAFT_TYPE_MAP = {
  tanker: "MILITARY_AIRCRAFT_TYPE_TANKER",
  awacs: "MILITARY_AIRCRAFT_TYPE_AWACS",
  transport: "MILITARY_AIRCRAFT_TYPE_TRANSPORT",
  reconnaissance: "MILITARY_AIRCRAFT_TYPE_RECONNAISSANCE",
  drone: "MILITARY_AIRCRAFT_TYPE_DRONE",
  bomber: "MILITARY_AIRCRAFT_TYPE_BOMBER"
};
async function listMilitaryFlights(ctx, req) {
  try {
    if (!req.neLat && !req.neLon && !req.swLat && !req.swLon) return { flights: [], clusters: [], pagination: void 0 };
    const requestBounds = normalizeBounds(req);
    const quantizedBB = [
      quantize(req.swLat, BBOX_GRID_STEP),
      quantize(req.swLon, BBOX_GRID_STEP),
      quantize(req.neLat, BBOX_GRID_STEP),
      quantize(req.neLon, BBOX_GRID_STEP)
    ].join(":");
    const cacheKey2 = `${REDIS_CACHE_KEY32}:${quantizedBB}:${req.operator || ""}:${req.aircraftType || ""}:${req.pageSize || 0}`;
    const fullResult = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL32,
      async () => {
        const isSidecar = (process.env.LOCAL_API_MODE || "").includes("sidecar");
        const baseUrl = isSidecar ? "https://opensky-network.org/api/states/all" : process.env.WS_RELAY_URL ? process.env.WS_RELAY_URL + "/opensky" : null;
        if (!baseUrl) return null;
        const fetchBB = {
          lamin: quantize(req.swLat, BBOX_GRID_STEP) - BBOX_GRID_STEP / 2,
          lamax: quantize(req.neLat, BBOX_GRID_STEP) + BBOX_GRID_STEP / 2,
          lomin: quantize(req.swLon, BBOX_GRID_STEP) - BBOX_GRID_STEP / 2,
          lomax: quantize(req.neLon, BBOX_GRID_STEP) + BBOX_GRID_STEP / 2
        };
        const params = new URLSearchParams();
        params.set("lamin", String(fetchBB.lamin));
        params.set("lamax", String(fetchBB.lamax));
        params.set("lomin", String(fetchBB.lomin));
        params.set("lomax", String(fetchBB.lomax));
        const url = `${baseUrl}${params.toString() ? "?" + params.toString() : ""}`;
        const resp = await fetch(url, {
          headers: getRelayRequestHeaders2(),
          signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS5)
        });
        if (!resp.ok) return null;
        const data = await resp.json();
        if (!data.states) return null;
        const flights = [];
        for (const state of data.states) {
          const [icao24, callsign, , , , lon, lat, altitude, onGround, velocity, heading] = state;
          if (lat == null || lon == null || onGround) continue;
          if (!isMilitaryCallsign(callsign) && !isMilitaryHex(icao24)) continue;
          const aircraftType = detectAircraftType(callsign);
          flights.push({
            id: icao24,
            callsign: (callsign || "").trim(),
            hexCode: icao24,
            registration: "",
            aircraftType: AIRCRAFT_TYPE_MAP[aircraftType] || "MILITARY_AIRCRAFT_TYPE_UNKNOWN",
            aircraftModel: "",
            operator: "MILITARY_OPERATOR_OTHER",
            operatorCountry: "",
            location: { latitude: lat, longitude: lon },
            altitude: altitude ?? 0,
            heading: heading ?? 0,
            speed: velocity ?? 0,
            verticalRate: 0,
            onGround: false,
            squawk: "",
            origin: "",
            destination: "",
            lastSeenAt: Date.now(),
            firstSeenAt: 0,
            confidence: "MILITARY_CONFIDENCE_LOW",
            isInteresting: false,
            note: "",
            enrichment: void 0
          });
        }
        return flights.length > 0 ? { flights, clusters: [], pagination: void 0 } : null;
      }
    );
    if (!fullResult) {
      markNoCacheResponse(ctx.request);
      return { flights: [], clusters: [], pagination: void 0 };
    }
    return { ...fullResult, flights: filterFlightsToBounds(fullResult.flights, requestBounds) };
  } catch {
    markNoCacheResponse(ctx.request);
    return { flights: [], clusters: [], pagination: void 0 };
  }
}

// server/worldmonitor/military/v1/get-theater-posture.ts
var CACHE_KEY4 = "theater-posture:sebuf:v1";
var STALE_CACHE_KEY = "theater-posture:sebuf:stale:v1";
var BACKUP_CACHE_KEY = "theater-posture:sebuf:backup:v1";
var CACHE_TTL4 = 900;
var STALE_TTL = 86400;
var BACKUP_TTL = 604800;
var WINGBITS_BACKOFF_MS = 5 * 60 * 1e3;
var wingbitsBackoffUntil = 0;
function getRelayRequestHeaders3() {
  const headers = {
    Accept: "application/json",
    "User-Agent": CHROME_UA
  };
  const relaySecret = process.env.RELAY_SHARED_SECRET;
  if (relaySecret) {
    const relayHeader = (process.env.RELAY_AUTH_HEADER || "x-relay-key").toLowerCase();
    headers[relayHeader] = relaySecret;
    headers.Authorization = `Bearer ${relaySecret}`;
  }
  return headers;
}
var THEATER_QUERY_REGIONS = [
  { name: "WESTERN", lamin: 10, lamax: 66, lomin: 9, lomax: 66 },
  // Baltic→Yemen, Baltic→Iran
  { name: "PACIFIC", lamin: 4, lamax: 44, lomin: 104, lomax: 133 }
  // SCS→Korea
];
function parseOpenSkyStates(data) {
  if (!data.states) return [];
  const flights = [];
  for (const state of data.states) {
    const [icao24, callsign, , , , lon, lat, altitude, onGround, velocity, heading] = state;
    if (lat == null || lon == null || onGround) continue;
    if (!isMilitaryCallsign(callsign) && !isMilitaryHex(icao24)) continue;
    flights.push({
      id: icao24,
      callsign: callsign?.trim() || "",
      lat,
      lon,
      altitude: altitude ?? 0,
      heading: heading ?? 0,
      speed: velocity ?? 0,
      aircraftType: detectAircraftType(callsign)
    });
  }
  return flights;
}
async function fetchMilitaryFlightsFromOpenSky() {
  const isSidecar = (process.env.LOCAL_API_MODE || "").includes("sidecar");
  const baseUrl = isSidecar ? "https://opensky-network.org/api/states/all" : process.env.WS_RELAY_URL ? process.env.WS_RELAY_URL + "/opensky" : null;
  if (!baseUrl) return [];
  const seenIds = /* @__PURE__ */ new Set();
  const allFlights = [];
  for (const region of THEATER_QUERY_REGIONS) {
    const params = `lamin=${region.lamin}&lamax=${region.lamax}&lomin=${region.lomin}&lomax=${region.lomax}`;
    const resp = await fetch(`${baseUrl}?${params}`, {
      headers: getRelayRequestHeaders3(),
      signal: AbortSignal.timeout(UPSTREAM_TIMEOUT_MS5)
    });
    if (!resp.ok) throw new Error(`OpenSky API error: ${resp.status} for ${region.name}`);
    const data = await resp.json();
    for (const flight of parseOpenSkyStates(data)) {
      if (!seenIds.has(flight.id)) {
        seenIds.add(flight.id);
        allFlights.push(flight);
      }
    }
  }
  return allFlights;
}
async function fetchMilitaryFlightsFromWingbits() {
  const apiKey = process.env.WINGBITS_API_KEY;
  if (!apiKey) return null;
  if (Date.now() < wingbitsBackoffUntil) {
    return null;
  }
  const areas = POSTURE_THEATERS.map((t) => ({
    alias: t.id,
    by: "box",
    la: (t.bounds.north + t.bounds.south) / 2,
    lo: (t.bounds.east + t.bounds.west) / 2,
    w: Math.abs(t.bounds.east - t.bounds.west) * 60,
    h: Math.abs(t.bounds.north - t.bounds.south) * 60,
    unit: "nm"
  }));
  try {
    const resp = await fetch("https://customer-api.wingbits.com/v1/flights", {
      method: "POST",
      headers: { "x-api-key": apiKey, Accept: "application/json", "Content-Type": "application/json", "User-Agent": CHROME_UA },
      body: JSON.stringify(areas),
      signal: AbortSignal.timeout(15e3)
    });
    if (!resp.ok) {
      console.warn(`[TheaterPosture] Wingbits ${resp.status} \u2014 backing off 5 min`);
      wingbitsBackoffUntil = Date.now() + WINGBITS_BACKOFF_MS;
      return null;
    }
    wingbitsBackoffUntil = 0;
    const data = await resp.json();
    const flights = [];
    const seenIds = /* @__PURE__ */ new Set();
    for (const areaResult of data) {
      const flightList = Array.isArray(areaResult.flights || areaResult) ? areaResult.flights || areaResult : [];
      for (const f of flightList) {
        const icao24 = f.h || f.icao24 || f.id;
        if (!icao24 || seenIds.has(icao24)) continue;
        seenIds.add(icao24);
        const callsign = (f.f || f.callsign || f.flight || "").trim();
        if (!isMilitaryCallsign(callsign) && !isMilitaryHex(icao24)) continue;
        flights.push({
          id: icao24,
          callsign,
          lat: f.la || f.latitude || f.lat,
          lon: f.lo || f.longitude || f.lon || f.lng,
          altitude: f.ab || f.altitude || f.alt || 0,
          heading: f.th || f.heading || f.track || 0,
          speed: f.gs || f.groundSpeed || f.speed || f.velocity || 0,
          aircraftType: detectAircraftType(callsign)
        });
      }
    }
    return flights;
  } catch {
    wingbitsBackoffUntil = Date.now() + WINGBITS_BACKOFF_MS;
    return null;
  }
}
function calculatePostures(flights) {
  return POSTURE_THEATERS.map((theater) => {
    const theaterFlights = flights.filter(
      (f) => f.lat >= theater.bounds.south && f.lat <= theater.bounds.north && f.lon >= theater.bounds.west && f.lon <= theater.bounds.east
    );
    const total = theaterFlights.length;
    const byType = {
      tankers: theaterFlights.filter((f) => f.aircraftType === "tanker").length,
      awacs: theaterFlights.filter((f) => f.aircraftType === "awacs").length,
      fighters: theaterFlights.filter((f) => f.aircraftType === "fighter").length
    };
    const postureLevel = total >= theater.thresholds.critical ? "critical" : total >= theater.thresholds.elevated ? "elevated" : "normal";
    const strikeCapable = byType.tankers >= theater.strikeIndicators.minTankers && byType.awacs >= theater.strikeIndicators.minAwacs && byType.fighters >= theater.strikeIndicators.minFighters;
    const ops = [];
    if (strikeCapable) ops.push("strike_capable");
    if (byType.tankers > 0) ops.push("aerial_refueling");
    if (byType.awacs > 0) ops.push("airborne_early_warning");
    return {
      theater: theater.id,
      postureLevel,
      activeFlights: total,
      trackedVessels: 0,
      activeOperations: ops,
      assessedAt: Date.now()
    };
  });
}
async function fetchTheaterPostureFresh() {
  let flights = [];
  try {
    flights = await fetchMilitaryFlightsFromOpenSky();
  } catch {
    flights = [];
  }
  if (flights.length === 0) {
    const wingbitsFlights = await fetchMilitaryFlightsFromWingbits();
    if (wingbitsFlights && wingbitsFlights.length > 0) {
      flights = wingbitsFlights;
    } else {
      throw new Error("Both OpenSky and Wingbits unavailable");
    }
  }
  const theaters = calculatePostures(flights);
  const result = { theaters };
  await Promise.all([
    setCachedJson(STALE_CACHE_KEY, result, STALE_TTL),
    setCachedJson(BACKUP_CACHE_KEY, result, BACKUP_TTL)
  ]);
  return result;
}
async function getTheaterPosture(_ctx, _req) {
  try {
    const result = await cachedFetchJson(
      CACHE_KEY4,
      CACHE_TTL4,
      fetchTheaterPostureFresh
    );
    if (result) return result;
  } catch {
  }
  const stale = await getCachedJson(STALE_CACHE_KEY);
  if (stale) return stale;
  const backup = await getCachedJson(BACKUP_CACHE_KEY);
  if (backup) return backup;
  return { theaters: [] };
}

// server/worldmonitor/military/v1/get-aircraft-details.ts
var REDIS_CACHE_KEY33 = "military:aircraft:v1";
var REDIS_CACHE_TTL33 = 24 * 60 * 60;
async function getAircraftDetails(_ctx, req) {
  if (!req.icao24) return { details: void 0, configured: false };
  const apiKey = process.env.WINGBITS_API_KEY;
  if (!apiKey) return { details: void 0, configured: false };
  const icao24 = req.icao24.toLowerCase();
  const cacheKey2 = `${REDIS_CACHE_KEY33}:${icao24}`;
  try {
    const result = await cachedFetchJson(cacheKey2, REDIS_CACHE_TTL33, async () => {
      const resp = await fetch(`https://customer-api.wingbits.com/v1/flights/details/${icao24}`, {
        headers: { "x-api-key": apiKey, Accept: "application/json", "User-Agent": CHROME_UA },
        signal: AbortSignal.timeout(1e4)
      });
      if (resp.status === 404) {
        return { details: null, configured: true };
      }
      if (!resp.ok) return null;
      const data = await resp.json();
      return {
        details: mapWingbitsDetails(icao24, data),
        configured: true
      };
    });
    if (!result || !result.details) {
      return { details: void 0, configured: true };
    }
    return {
      details: result.details,
      configured: true
    };
  } catch {
    return { details: void 0, configured: true };
  }
}

// server/worldmonitor/military/v1/get-aircraft-details-batch.ts
async function getAircraftDetailsBatch(_ctx, req) {
  try {
    const apiKey = process.env.WINGBITS_API_KEY;
    if (!apiKey) return { results: {}, fetched: 0, requested: 0, configured: false };
    const normalized = req.icao24s.map((id) => id.trim().toLowerCase()).filter((id) => id.length > 0);
    const uniqueSorted = Array.from(new Set(normalized)).sort();
    const limitedList = uniqueSorted.slice(0, 10);
    const SINGLE_KEY = "military:aircraft:v1";
    const SINGLE_TTL = 24 * 60 * 60;
    const results = {};
    const toFetch = [];
    const cacheKeys = limitedList.map((icao24) => `${SINGLE_KEY}:${icao24}`);
    const cachedMap = await getCachedJsonBatch(cacheKeys);
    for (let i = 0; i < limitedList.length; i++) {
      const icao24 = limitedList[i];
      const cached = cachedMap.get(cacheKeys[i]);
      if (cached && typeof cached === "object" && "details" in cached) {
        const details = cached.details;
        if (details) {
          results[icao24] = details;
        }
      } else {
        toFetch.push(icao24);
      }
    }
    const delay = (ms) => new Promise((r) => setTimeout(r, ms));
    for (let i = 0; i < toFetch.length; i++) {
      const icao24 = toFetch[i];
      const cacheResult = await cachedFetchJson(
        `${SINGLE_KEY}:${icao24}`,
        SINGLE_TTL,
        async () => {
          try {
            const resp = await fetch(`https://customer-api.wingbits.com/v1/flights/details/${icao24}`, {
              headers: { "x-api-key": apiKey, Accept: "application/json", "User-Agent": CHROME_UA },
              signal: AbortSignal.timeout(1e4)
            });
            if (resp.status === 404) {
              return { details: null, configured: true };
            }
            if (resp.ok) {
              const data = await resp.json();
              const details = mapWingbitsDetails(icao24, data);
              return { details, configured: true };
            }
          } catch {
          }
          return null;
        }
      );
      if (cacheResult?.details) results[icao24] = cacheResult.details;
      if (i < toFetch.length - 1) await delay(100);
    }
    return {
      results,
      fetched: Object.keys(results).length,
      requested: limitedList.length,
      configured: true
    };
  } catch {
    return { results: {}, fetched: 0, requested: 0, configured: true };
  }
}

// server/worldmonitor/military/v1/get-wingbits-status.ts
async function getWingbitsStatus(_ctx, _req) {
  const apiKey = process.env.WINGBITS_API_KEY;
  return { configured: !!apiKey };
}

// server/worldmonitor/military/v1/get-usni-fleet-report.ts
var USNI_CACHE_KEY = "usni-fleet:sebuf:v1";
var USNI_STALE_CACHE_KEY = "usni-fleet:sebuf:stale:v1";
var USNI_CACHE_TTL = 21600;
var USNI_STALE_TTL = 604800;
var HULL_TYPE_MAP = {
  CVN: "carrier",
  CV: "carrier",
  DDG: "destroyer",
  CG: "destroyer",
  LHD: "amphibious",
  LHA: "amphibious",
  LPD: "amphibious",
  LSD: "amphibious",
  LCC: "amphibious",
  SSN: "submarine",
  SSBN: "submarine",
  SSGN: "submarine",
  FFG: "frigate",
  LCS: "frigate",
  MCM: "patrol",
  PC: "patrol",
  AS: "auxiliary",
  ESB: "auxiliary",
  ESD: "auxiliary",
  "T-AO": "auxiliary",
  "T-AKE": "auxiliary",
  "T-AOE": "auxiliary",
  "T-ARS": "auxiliary",
  "T-ESB": "auxiliary",
  "T-EPF": "auxiliary",
  "T-AGOS": "research",
  "T-AGS": "research",
  "T-AGM": "research",
  AGOS: "research"
};
function hullToVesselType(hull) {
  if (!hull) return "unknown";
  for (const [prefix, type] of Object.entries(HULL_TYPE_MAP)) {
    if (hull.startsWith(prefix)) return type;
  }
  return "unknown";
}
function detectDeploymentStatus(text) {
  if (!text) return "unknown";
  const lower = text.toLowerCase();
  if (lower.includes("deployed") || lower.includes("deployment")) return "deployed";
  if (lower.includes("underway") || lower.includes("transiting") || lower.includes("transit")) return "underway";
  if (lower.includes("homeport") || lower.includes("in port") || lower.includes("pierside") || lower.includes("returned")) return "in-port";
  return "unknown";
}
function extractHomePort(text) {
  const match = text.match(/homeported (?:at|in) ([^.,]+)/i) || text.match(/home[ -]?ported (?:at|in) ([^.,]+)/i);
  return match ? match[1].trim() : void 0;
}
function stripHtml(html) {
  return html.replace(/<[^>]+>/g, " ").replace(/&nbsp;/g, " ").replace(/&amp;/g, "&").replace(/&lt;/g, "<").replace(/&gt;/g, ">").replace(/&#8217;/g, "'").replace(/&#8220;/g, '"').replace(/&#8221;/g, '"').replace(/&#8211;/g, "\u2013").replace(/\s+/g, " ").trim();
}
var REGION_COORDS = {
  "Philippine Sea": { lat: 18, lon: 130 },
  "South China Sea": { lat: 14, lon: 115 },
  "East China Sea": { lat: 28, lon: 125 },
  "Sea of Japan": { lat: 40, lon: 135 },
  "Arabian Sea": { lat: 18, lon: 63 },
  "Red Sea": { lat: 20, lon: 38 },
  "Mediterranean Sea": { lat: 35, lon: 18 },
  "Eastern Mediterranean": { lat: 34.5, lon: 33 },
  "Western Mediterranean": { lat: 37, lon: 3 },
  "Persian Gulf": { lat: 26.5, lon: 52 },
  "Gulf of Oman": { lat: 24.5, lon: 58.5 },
  "Gulf of Aden": { lat: 12, lon: 47 },
  "Caribbean Sea": { lat: 15, lon: -73 },
  "North Atlantic": { lat: 45, lon: -30 },
  "Atlantic Ocean": { lat: 30, lon: -40 },
  "Western Atlantic": { lat: 30, lon: -60 },
  "Pacific Ocean": { lat: 20, lon: -150 },
  "Eastern Pacific": { lat: 18, lon: -125 },
  "Western Pacific": { lat: 20, lon: 140 },
  "Indian Ocean": { lat: -5, lon: 75 },
  Antarctic: { lat: -70, lon: 20 },
  "Baltic Sea": { lat: 58, lon: 20 },
  "Black Sea": { lat: 43.5, lon: 34 },
  "Bay of Bengal": { lat: 14, lon: 87 },
  Yokosuka: { lat: 35.29, lon: 139.67 },
  Japan: { lat: 35.29, lon: 139.67 },
  Sasebo: { lat: 33.16, lon: 129.72 },
  Guam: { lat: 13.45, lon: 144.79 },
  "Pearl Harbor": { lat: 21.35, lon: -157.95 },
  "San Diego": { lat: 32.68, lon: -117.15 },
  Norfolk: { lat: 36.95, lon: -76.3 },
  Mayport: { lat: 30.39, lon: -81.4 },
  Bahrain: { lat: 26.23, lon: 50.55 },
  Rota: { lat: 36.63, lon: -6.35 },
  "Diego Garcia": { lat: -7.32, lon: 72.42 },
  Djibouti: { lat: 11.55, lon: 43.15 },
  Singapore: { lat: 1.35, lon: 103.82 },
  "Souda Bay": { lat: 35.49, lon: 24.08 },
  Naples: { lat: 40.84, lon: 14.25 }
};
function getRegionCoords(regionText) {
  const normalized = regionText.replace(/^(In the|In|The)\s+/i, "").replace(/\s+/g, " ").trim();
  if (REGION_COORDS[normalized]) return REGION_COORDS[normalized];
  const lower = normalized.toLowerCase();
  for (const [key, coords] of Object.entries(REGION_COORDS)) {
    if (key.toLowerCase() === lower || lower.includes(key.toLowerCase()) || key.toLowerCase().includes(lower)) {
      return coords;
    }
  }
  return null;
}
function parseLeadingInteger(text) {
  const match = text.match(/\d{1,3}(?:,\d{3})*/);
  if (!match) return void 0;
  return parseInt(match[0].replace(/,/g, ""), 10);
}
function extractBattleForceSummary(tableHtml) {
  const rows = Array.from(tableHtml.matchAll(/<tr[^>]*>([\s\S]*?)<\/tr>/gi));
  if (rows.length < 2) return void 0;
  const headerCells = Array.from(rows[0][1].matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)).map((m) => stripHtml(m[1]).toLowerCase());
  const valueCells = Array.from(rows[1][1].matchAll(/<t[dh][^>]*>([\s\S]*?)<\/t[dh]>/gi)).map((m) => parseLeadingInteger(stripHtml(m[1])));
  const summary = { totalShips: 0, deployed: 0, underway: 0 };
  let matched = false;
  for (let idx = 0; idx < headerCells.length; idx++) {
    const label = headerCells[idx] || "";
    const value = valueCells[idx];
    if (!Number.isFinite(value)) continue;
    if (label.includes("battle force") || label.includes("total") || label.includes("ships")) {
      summary.totalShips = value;
      matched = true;
    } else if (label.includes("deployed")) {
      summary.deployed = value;
      matched = true;
    } else if (label.includes("underway")) {
      summary.underway = value;
      matched = true;
    }
  }
  if (matched) return summary;
  const tableText = stripHtml(tableHtml);
  const totalMatch = tableText.match(/(?:battle[- ]?force|ships?|total)[^0-9]{0,40}(\d{1,3}(?:,\d{3})*)/i) || tableText.match(/(\d{1,3}(?:,\d{3})*)\s*(?:battle[- ]?force|ships?|total)/i);
  const deployedMatch = tableText.match(/deployed[^0-9]{0,40}(\d{1,3}(?:,\d{3})*)/i) || tableText.match(/(\d{1,3}(?:,\d{3})*)\s*deployed/i);
  const underwayMatch = tableText.match(/underway[^0-9]{0,40}(\d{1,3}(?:,\d{3})*)/i) || tableText.match(/(\d{1,3}(?:,\d{3})*)\s*underway/i);
  if (!totalMatch && !deployedMatch && !underwayMatch) return void 0;
  return {
    totalShips: totalMatch ? parseInt(totalMatch[1].replace(/,/g, ""), 10) : 0,
    deployed: deployedMatch ? parseInt(deployedMatch[1].replace(/,/g, ""), 10) : 0,
    underway: underwayMatch ? parseInt(underwayMatch[1].replace(/,/g, ""), 10) : 0
  };
}
function parseUSNIArticle(html, articleUrl, articleDate, articleTitle) {
  const warnings = [];
  const vessels = [];
  const vesselByRegionHull = /* @__PURE__ */ new Map();
  const strikeGroups = [];
  const regionsSet = /* @__PURE__ */ new Set();
  let battleForceSummary;
  const tableMatch = html.match(/<table[^>]*>([\s\S]*?)<\/table>/i);
  if (tableMatch) {
    battleForceSummary = extractBattleForceSummary(tableMatch[1]);
  }
  const h2Parts = html.split(/<h2[^>]*>/i);
  for (let i = 1; i < h2Parts.length; i++) {
    const part = h2Parts[i];
    const h2EndIdx = part.indexOf("</h2>");
    if (h2EndIdx === -1) continue;
    const regionRaw = stripHtml(part.substring(0, h2EndIdx));
    const regionContent = part.substring(h2EndIdx + 5);
    const regionName = regionRaw.replace(/^(In the|In|The)\s+/i, "").replace(/\s+/g, " ").trim();
    if (!regionName) continue;
    regionsSet.add(regionName);
    const coords = getRegionCoords(regionName);
    if (!coords) {
      warnings.push(`Unknown region: "${regionName}"`);
    }
    const regionLat = coords?.lat ?? 0;
    const regionLon = coords?.lon ?? 0;
    const h3Parts = regionContent.split(/<h3[^>]*>/i);
    let currentStrikeGroup = null;
    for (let j = 0; j < h3Parts.length; j++) {
      const section = h3Parts[j];
      if (j > 0) {
        const h3EndIdx = section.indexOf("</h3>");
        if (h3EndIdx !== -1) {
          const sgName = stripHtml(section.substring(0, h3EndIdx));
          if (sgName) {
            currentStrikeGroup = {
              name: sgName,
              carrier: void 0,
              airWing: void 0,
              destroyerSquadron: void 0,
              escorts: []
            };
            strikeGroups.push(currentStrikeGroup);
          }
        }
      }
      const shipRegex = /USS\s+<(?:em|i)>([^<]+)<\/(?:em|i)>\s*\(([^)]+)\)/gi;
      let match;
      const sectionText = stripHtml(section);
      const deploymentStatus = detectDeploymentStatus(sectionText);
      const homePort = extractHomePort(sectionText);
      const activityDesc = sectionText.length > 10 ? sectionText.substring(0, 200).trim() : "";
      const upsertVessel = (entry) => {
        const key = `${entry.region}|${entry.hullNumber.toUpperCase()}`;
        const existing = vesselByRegionHull.get(key);
        if (existing) {
          if (!existing.strikeGroup && entry.strikeGroup) existing.strikeGroup = entry.strikeGroup;
          if (existing.deploymentStatus === "unknown" && entry.deploymentStatus !== "unknown") {
            existing.deploymentStatus = entry.deploymentStatus;
          }
          if (!existing.homePort && entry.homePort) existing.homePort = entry.homePort;
          if ((!existing.activityDescription || existing.activityDescription.length < (entry.activityDescription || "").length) && entry.activityDescription) {
            existing.activityDescription = entry.activityDescription;
          }
          return;
        }
        vessels.push(entry);
        vesselByRegionHull.set(key, entry);
      };
      while ((match = shipRegex.exec(section)) !== null) {
        const shipName = match[1].trim();
        const hullNumber = match[2].trim();
        const vesselType = hullToVesselType(hullNumber);
        if (vesselType === "carrier" && currentStrikeGroup) {
          currentStrikeGroup.carrier = `USS ${shipName} (${hullNumber})`;
        }
        if (currentStrikeGroup) {
          currentStrikeGroup.escorts.push(`USS ${shipName} (${hullNumber})`);
        }
        upsertVessel({
          name: `USS ${shipName}`,
          hullNumber,
          vesselType,
          region: regionName,
          regionLat,
          regionLon,
          deploymentStatus,
          homePort: homePort || "",
          strikeGroup: currentStrikeGroup?.name || "",
          activityDescription: activityDesc,
          articleUrl,
          articleDate
        });
      }
      const usnsRegex = /USNS\s+<(?:em|i)>([^<]+)<\/(?:em|i)>\s*\(([^)]+)\)/gi;
      while ((match = usnsRegex.exec(section)) !== null) {
        const shipName = match[1].trim();
        const hullNumber = match[2].trim();
        upsertVessel({
          name: `USNS ${shipName}`,
          hullNumber,
          vesselType: hullToVesselType(hullNumber),
          region: regionName,
          regionLat,
          regionLon,
          deploymentStatus,
          homePort: homePort || "",
          strikeGroup: currentStrikeGroup?.name || "",
          activityDescription: activityDesc,
          articleUrl,
          articleDate
        });
      }
    }
  }
  for (const sg of strikeGroups) {
    const wingMatch = html.match(new RegExp(sg.name + "[\\s\\S]{0,500}Carrier Air Wing\\s*(\\w+)", "i"));
    if (wingMatch) sg.airWing = `Carrier Air Wing ${wingMatch[1]}`;
    const desronMatch = html.match(new RegExp(sg.name + "[\\s\\S]{0,500}Destroyer Squadron\\s*(\\w+)", "i"));
    if (desronMatch) sg.destroyerSquadron = `Destroyer Squadron ${desronMatch[1]}`;
    sg.escorts = Array.from(new Set(sg.escorts));
  }
  const protoStrikeGroups = strikeGroups.map((sg) => ({
    name: sg.name,
    carrier: sg.carrier || "",
    airWing: sg.airWing || "",
    destroyerSquadron: sg.destroyerSquadron || "",
    escorts: sg.escorts
  }));
  return {
    articleUrl,
    articleDate,
    articleTitle,
    battleForceSummary,
    vessels,
    strikeGroups: protoStrikeGroups,
    regions: Array.from(regionsSet),
    parsingWarnings: warnings,
    timestamp: Date.now()
  };
}
async function fetchUSNIReport() {
  console.log("[USNI Fleet] Fetching from WordPress API...");
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 15e3);
  let wpData;
  try {
    const response = await fetch(
      "https://news.usni.org/wp-json/wp/v2/posts?categories=4137&per_page=1",
      {
        headers: { Accept: "application/json", "User-Agent": CHROME_UA },
        signal: controller.signal
      }
    );
    if (!response.ok) throw new Error(`USNI API error: ${response.status}`);
    wpData = await response.json();
  } finally {
    clearTimeout(timeoutId);
  }
  if (!wpData || !wpData.length) return null;
  const post = wpData[0];
  const articleUrl = post.link || `https://news.usni.org/?p=${post.id}`;
  const articleDate = post.date || (/* @__PURE__ */ new Date()).toISOString();
  const articleTitle = stripHtml(post.title?.rendered || "USNI Fleet Tracker");
  const htmlContent = post.content?.rendered || "";
  if (!htmlContent) return null;
  const report = parseUSNIArticle(htmlContent, articleUrl, articleDate, articleTitle);
  console.log(`[USNI Fleet] Parsed: ${report.vessels.length} vessels, ${report.strikeGroups.length} CSGs, ${report.regions.length} regions`);
  if (report.parsingWarnings.length > 0) {
    console.warn("[USNI Fleet] Warnings:", report.parsingWarnings.join("; "));
  }
  await setCachedJson(USNI_STALE_CACHE_KEY, report, USNI_STALE_TTL);
  return report;
}
async function getUSNIFleetReport(_ctx, req) {
  try {
    if (req.forceRefresh) {
      const report2 = await fetchUSNIReport();
      if (!report2) return { report: void 0, cached: false, stale: false, error: "No USNI fleet tracker articles found" };
      await setCachedJson(USNI_CACHE_KEY, report2, USNI_CACHE_TTL);
      return { report: report2, cached: false, stale: false, error: "" };
    }
    const { data: report, source } = await cachedFetchJsonWithMeta(
      USNI_CACHE_KEY,
      USNI_CACHE_TTL,
      fetchUSNIReport
    );
    if (report) {
      if (source === "cache") console.log("[USNI Fleet] Cache hit");
      return { report, cached: source === "cache", stale: false, error: "" };
    }
    return { report: void 0, cached: false, stale: false, error: "No USNI fleet tracker articles found" };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    console.warn("[USNI Fleet] Error:", message);
    const stale = await getCachedJson(USNI_STALE_CACHE_KEY);
    if (stale) {
      console.log("[USNI Fleet] Returning stale cached data");
      return { report: stale, cached: true, stale: true, error: "Using cached data" };
    }
    return { report: void 0, cached: false, stale: false, error: message };
  }
}

// server/worldmonitor/military/v1/list-military-bases.ts
var VALID_TYPES = /* @__PURE__ */ new Set([
  "us-nato",
  "china",
  "russia",
  "uk",
  "france",
  "india",
  "italy",
  "uae",
  "turkey",
  "japan",
  "other"
]);
var VALID_KINDS = /* @__PURE__ */ new Set([
  "base",
  "airfield",
  "naval_base",
  "military",
  "barracks",
  "bunker",
  "trench",
  "training_area",
  "checkpoint",
  "shelter",
  "ammunition",
  "office",
  "obstacle_course",
  "nuclear_explosion_site",
  "range"
]);
var COUNTRY_RE = /^[A-Z]{2}$/;
var quantize2 = (v, step) => Math.round(v / step) * step;
function getBboxGridStep(zoom) {
  if (zoom < 5) return 5;
  if (zoom <= 7) return 1;
  return 0.5;
}
function haversineDistKm(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = (lat2 - lat1) * Math.PI / 180;
  const dLon = (lon2 - lon1) * Math.PI / 180;
  const a = Math.sin(dLat / 2) ** 2 + Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}
function bboxDimensionsKm(swLat, swLon, neLat, neLon) {
  const centerLat = (swLat + neLat) / 2;
  const centerLon = (swLon + neLon) / 2;
  const heightKm = haversineDistKm(swLat, centerLon, neLat, centerLon);
  const widthKm = haversineDistKm(centerLat, swLon, centerLat, neLon);
  return { centerLat, centerLon, widthKm: Math.max(widthKm, 1), heightKm: Math.max(heightKm, 1) };
}
function getGeoSearchCap(zoom) {
  if (zoom < 5) return 2e3;
  if (zoom <= 7) return 5e3;
  return 1e4;
}
function getClusterCellSize(zoom) {
  if (zoom < 4) return 5;
  if (zoom < 6) return 2;
  if (zoom < 8) return 0.5;
  return 0;
}
function clusterBases(bases, cellSize) {
  if (cellSize === 0 || bases.length <= 200) return { entries: bases, clusters: [] };
  const cells = /* @__PURE__ */ new Map();
  for (const b of bases) {
    const ck = `${Math.floor(b.latitude / cellSize)}:${Math.floor(b.longitude / cellSize)}`;
    let arr = cells.get(ck);
    if (!arr) {
      arr = [];
      cells.set(ck, arr);
    }
    arr.push(b);
  }
  const entries = [];
  const clusters = [];
  for (const group of cells.values()) {
    if (group.length === 1) {
      entries.push(group[0]);
      continue;
    }
    let latSum = 0, lonSum = 0;
    const typeCounts = /* @__PURE__ */ new Map();
    for (const b of group) {
      latSum += b.latitude;
      lonSum += b.longitude;
      typeCounts.set(b.type, (typeCounts.get(b.type) || 0) + 1);
    }
    let dominantType = "other";
    let maxCount = 0;
    for (const [t, c] of typeCounts) {
      if (c > maxCount) {
        maxCount = c;
        dominantType = t;
      }
    }
    clusters.push({
      latitude: latSum / group.length,
      longitude: lonSum / group.length,
      count: group.length,
      dominantType,
      expansionZoom: cellSize >= 2 ? 6 : cellSize >= 0.5 ? 8 : 10
    });
  }
  return { entries, clusters };
}
async function listMilitaryBases(ctx, req) {
  try {
    const empty = { bases: [], clusters: [], totalInView: 0, truncated: false };
    if (!req.neLat && !req.neLon && !req.swLat && !req.swLon) return empty;
    const swLat = Math.max(-90, Math.min(90, req.swLat));
    const neLat = Math.max(-90, Math.min(90, req.neLat));
    const swLon = Math.max(-180, Math.min(180, req.swLon));
    const neLon = Math.max(-180, Math.min(180, req.neLon));
    const zoom = Math.max(0, Math.min(22, req.zoom || 3));
    const typeFilter = req.type ? req.type.toLowerCase().trim().slice(0, 20) : "";
    const kindFilter = req.kind ? req.kind.toLowerCase().trim().slice(0, 20) : "";
    const countryFilter = req.country ? req.country.toUpperCase().trim().slice(0, 20) : "";
    if (typeFilter && !VALID_TYPES.has(typeFilter)) return empty;
    if (kindFilter && !VALID_KINDS.has(kindFilter)) return empty;
    if (countryFilter && !COUNTRY_RE.test(countryFilter)) return empty;
    let activeVersion = await getCachedJson("military:bases:active");
    let rawKeys = false;
    if (!activeVersion) {
      activeVersion = await getCachedJson("military:bases:active", true);
      rawKeys = true;
    }
    if (!activeVersion) {
      markNoCacheResponse(ctx.request);
      setResponseHeader(ctx.request, "X-Bases-Debug", "no-active-version");
      console.warn("military:bases:active key missing \u2014 run seed script");
      return empty;
    }
    const v = String(activeVersion);
    setResponseHeader(ctx.request, "X-Bases-Debug", `v=${v},raw=${rawKeys}`);
    const geoKey = `military:bases:geo:${v}`;
    const metaKey = `military:bases:meta:${v}`;
    const gridStep = getBboxGridStep(zoom);
    const qBB = [
      quantize2(swLat, gridStep),
      quantize2(swLon, gridStep),
      quantize2(neLat, gridStep),
      quantize2(neLon, gridStep)
    ].join(":");
    const cacheKey2 = `military:bases:v1:${qBB}:${zoom}:${typeFilter}:${kindFilter}:${countryFilter}:${v}`;
    const result = await cachedFetchJson(
      cacheKey2,
      3600,
      async () => {
        const antimeridian = swLon > neLon;
        let allIds;
        if (antimeridian) {
          const dims1 = bboxDimensionsKm(swLat, swLon, neLat, 180);
          const dims2 = bboxDimensionsKm(swLat, -180, neLat, neLon);
          const cap = getGeoSearchCap(zoom);
          const [ids1, ids2] = await Promise.all([
            geoSearchByBox(geoKey, dims1.centerLon, dims1.centerLat, dims1.widthKm, dims1.heightKm, cap, rawKeys),
            geoSearchByBox(geoKey, dims2.centerLon, dims2.centerLat, dims2.widthKm, dims2.heightKm, cap, rawKeys)
          ]);
          const seen = /* @__PURE__ */ new Set();
          allIds = [];
          for (const id of [...ids1, ...ids2]) {
            if (!seen.has(id)) {
              seen.add(id);
              allIds.push(id);
            }
          }
        } else {
          const dims = bboxDimensionsKm(swLat, swLon, neLat, neLon);
          const cap = getGeoSearchCap(zoom);
          allIds = await geoSearchByBox(geoKey, dims.centerLon, dims.centerLat, dims.widthKm, dims.heightKm, cap, rawKeys);
        }
        const truncated = allIds.length >= getGeoSearchCap(zoom);
        if (allIds.length === 0) return { bases: [], clusters: [], totalInView: 0, truncated: false };
        const metaMap = await getHashFieldsBatch(metaKey, allIds, rawKeys);
        const bases = [];
        for (const id of allIds) {
          const raw = metaMap.get(id);
          if (!raw) continue;
          let meta;
          try {
            meta = JSON.parse(raw);
          } catch {
            continue;
          }
          const tier = meta.tier || 2;
          if (zoom < 5 && tier > 1) continue;
          if (zoom >= 5 && zoom < 8 && tier > 2) continue;
          if (typeFilter && meta.type !== typeFilter) continue;
          if (kindFilter && meta.kind !== kindFilter) continue;
          if (countryFilter && meta.countryIso2 !== countryFilter) continue;
          bases.push({
            id: String(meta.id || id),
            name: String(meta.name || ""),
            latitude: Number(meta.lat) || 0,
            longitude: Number(meta.lon) || 0,
            kind: String(meta.kind || ""),
            countryIso2: String(meta.countryIso2 || ""),
            type: String(meta.type || "other"),
            tier,
            catAirforce: Boolean(meta.catAirforce),
            catNaval: Boolean(meta.catNaval),
            catNuclear: Boolean(meta.catNuclear),
            catSpace: Boolean(meta.catSpace),
            catTraining: Boolean(meta.catTraining),
            branch: String(meta.branch || ""),
            status: String(meta.status || "")
          });
        }
        const cellSize = getClusterCellSize(zoom);
        const { entries, clusters } = clusterBases(bases, cellSize);
        return {
          bases: entries,
          clusters,
          totalInView: bases.length,
          truncated
        };
      }
    );
    if (!result) {
      markNoCacheResponse(ctx.request);
      return empty;
    }
    return result;
  } catch (err) {
    markNoCacheResponse(ctx.request);
    setResponseHeader(ctx.request, "X-Bases-Debug", `error:${err instanceof Error ? err.message : String(err)}`);
    return { bases: [], clusters: [], totalInView: 0, truncated: false };
  }
}

// server/worldmonitor/military/v1/handler.ts
var militaryHandler = {
  listMilitaryFlights,
  getTheaterPosture,
  getAircraftDetails,
  getAircraftDetailsBatch,
  getWingbitsStatus,
  getUSNIFleetReport,
  listMilitaryBases
};

// src/generated/server/worldmonitor/positive_events/v1/service_server.ts
var ValidationError18 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createPositiveEventsServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/positive-events/v1/list-positive-geo-events",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.listPositiveGeoEvents(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError18) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// src/services/positive-classifier.ts
var SOURCE_CATEGORY_MAP = {
  "GNN Science": "science-health",
  "GNN Health": "science-health",
  "GNN Animals": "nature-wildlife",
  "GNN Heroes": "humanity-kindness"
};
var CATEGORY_KEYWORDS = [
  // Science & Health (most specific first)
  ["clinical trial", "science-health"],
  ["study finds", "science-health"],
  ["researchers", "science-health"],
  ["scientists", "science-health"],
  ["breakthrough", "science-health"],
  ["discovery", "science-health"],
  ["cure", "science-health"],
  ["vaccine", "science-health"],
  ["treatment", "science-health"],
  ["medical", "science-health"],
  ["therapy", "science-health"],
  ["cancer", "science-health"],
  ["disease", "science-health"],
  // Nature & Wildlife
  ["endangered species", "nature-wildlife"],
  ["conservation", "nature-wildlife"],
  ["wildlife", "nature-wildlife"],
  ["species", "nature-wildlife"],
  ["marine", "nature-wildlife"],
  ["reef", "nature-wildlife"],
  ["forest", "nature-wildlife"],
  ["whale", "nature-wildlife"],
  ["bird", "nature-wildlife"],
  ["animal", "nature-wildlife"],
  // Climate Wins (before innovation so "solar" matches climate, not tech)
  ["renewable", "climate-wins"],
  ["solar", "climate-wins"],
  ["wind energy", "climate-wins"],
  ["wind farm", "climate-wins"],
  ["electric vehicle", "climate-wins"],
  ["emissions", "climate-wins"],
  ["carbon", "climate-wins"],
  ["clean energy", "climate-wins"],
  ["climate", "climate-wins"],
  ["green hydrogen", "climate-wins"],
  // Innovation & Tech
  ["robot", "innovation-tech"],
  ["technology", "innovation-tech"],
  ["startup", "innovation-tech"],
  ["invention", "innovation-tech"],
  ["innovation", "innovation-tech"],
  ["engineering", "innovation-tech"],
  ["3d print", "innovation-tech"],
  ["artificial intelligence", "innovation-tech"],
  [" ai ", "innovation-tech"],
  // Humanity & Kindness
  ["volunteer", "humanity-kindness"],
  ["donated", "humanity-kindness"],
  ["charity", "humanity-kindness"],
  ["rescued", "humanity-kindness"],
  ["hero", "humanity-kindness"],
  ["kindness", "humanity-kindness"],
  ["helping", "humanity-kindness"],
  ["community", "humanity-kindness"],
  // Culture & Community
  [" art ", "culture-community"],
  ["music", "culture-community"],
  ["festival", "culture-community"],
  ["cultural", "culture-community"],
  ["education", "culture-community"],
  ["school", "culture-community"],
  ["library", "culture-community"],
  ["museum", "culture-community"]
];
function classifyPositiveContent(title) {
  const lower = ` ${title.toLowerCase()} `;
  for (const [keyword, category] of CATEGORY_KEYWORDS) {
    if (lower.includes(keyword)) return category;
  }
  return "humanity-kindness";
}
function classifyNewsItem(source, title) {
  const sourceCategory = SOURCE_CATEGORY_MAP[source];
  if (sourceCategory) return sourceCategory;
  return classifyPositiveContent(title);
}

// server/worldmonitor/positive-events/v1/list-positive-geo-events.ts
var GDELT_GEO_URL2 = "https://api.gdeltproject.org/api/v2/geo/geo";
var REDIS_CACHE_KEY34 = "positive-events:geo:v1";
var REDIS_CACHE_TTL34 = 900;
var POSITIVE_QUERIES = [
  '(breakthrough OR discovery OR "renewable energy")',
  '(conservation OR "poverty decline" OR "humanitarian aid")',
  '("good news" OR volunteer OR donation OR charity)'
];
async function fetchGdeltGeoPositive(query) {
  const params = new URLSearchParams({
    query,
    format: "geojson",
    timespan: "24h",
    maxrecords: "75"
  });
  const response = await fetch(`${GDELT_GEO_URL2}?${params}`, {
    headers: { Accept: "application/json" },
    signal: AbortSignal.timeout(1e4)
  });
  if (!response.ok) return [];
  const data = await response.json();
  const features = data?.features || [];
  const seenLocations = /* @__PURE__ */ new Set();
  const events = [];
  for (const feature of features) {
    const name = feature.properties?.name || "";
    if (!name || seenLocations.has(name)) continue;
    if (name.startsWith("ERROR:") || name.includes("unknown error")) continue;
    const count = feature.properties?.count || 1;
    if (count < 3) continue;
    const coords = feature.geometry?.coordinates;
    if (!Array.isArray(coords) || coords.length < 2) continue;
    const [lon, lat] = coords;
    if (!Number.isFinite(lat) || !Number.isFinite(lon) || lat < -90 || lat > 90 || lon < -180 || lon > 180) continue;
    seenLocations.add(name);
    const category = classifyNewsItem("GDELT", name);
    events.push({
      latitude: lat,
      longitude: lon,
      name,
      category,
      count,
      timestamp: Date.now()
    });
  }
  return events;
}
async function listPositiveGeoEvents(ctx, _req) {
  try {
    const result = await cachedFetchJson(REDIS_CACHE_KEY34, REDIS_CACHE_TTL34, async () => {
      const allEvents = [];
      const seenNames = /* @__PURE__ */ new Set();
      let anyQuerySucceeded = false;
      for (let i = 0; i < POSITIVE_QUERIES.length; i++) {
        if (i > 0) {
          await new Promise((r) => setTimeout(r, 500));
        }
        try {
          const events = await fetchGdeltGeoPositive(POSITIVE_QUERIES[i]);
          anyQuerySucceeded = true;
          for (const event of events) {
            if (!seenNames.has(event.name)) {
              seenNames.add(event.name);
              allEvents.push(event);
            }
          }
        } catch {
        }
      }
      return anyQuerySucceeded ? { events: allEvents } : null;
    });
    return result || { events: [] };
  } catch {
    markNoCacheResponse(ctx.request);
    return { events: [] };
  }
}

// server/worldmonitor/positive-events/v1/handler.ts
var positiveEventsHandler = {
  listPositiveGeoEvents
};

// src/generated/server/worldmonitor/giving/v1/service_server.ts
var ValidationError19 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createGivingServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/giving/v1/get-giving-summary",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            platformLimit: Number(params.get("platform_limit") ?? "0"),
            categoryLimit: Number(params.get("category_limit") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getGivingSummary", body);
            if (bodyViolations) {
              throw new ValidationError19(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getGivingSummary(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError19) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/giving/v1/get-giving-summary.ts
var REDIS_CACHE_KEY35 = "giving:summary:v1";
var REDIS_CACHE_TTL35 = 3600;
function getGoFundMeEstimate() {
  return {
    platform: "GoFundMe",
    dailyVolumeUsd: 9e9 / 365,
    // ~$24.7M/day from 2024 annual report
    activeCampaignsSampled: 0,
    newCampaigns24h: 0,
    donationVelocity: 0,
    dataFreshness: "annual",
    lastUpdated: (/* @__PURE__ */ new Date()).toISOString()
  };
}
function getGlobalGivingEstimate() {
  return {
    platform: "GlobalGiving",
    dailyVolumeUsd: 1e8 / 365,
    // ~$274K/day from annual reports
    activeCampaignsSampled: 0,
    newCampaigns24h: 0,
    donationVelocity: 0,
    dataFreshness: "annual",
    lastUpdated: (/* @__PURE__ */ new Date()).toISOString()
  };
}
function getJustGivingEstimate() {
  return {
    platform: "JustGiving",
    dailyVolumeUsd: 7e9 / 365,
    // ~$19.2M/day from annual reports
    activeCampaignsSampled: 0,
    newCampaigns24h: 0,
    donationVelocity: 0,
    dataFreshness: "annual",
    lastUpdated: (/* @__PURE__ */ new Date()).toISOString()
  };
}
function getCryptoGivingEstimate() {
  return {
    dailyInflowUsd: 2e9 / 365,
    // ~$5.5M/day estimate
    trackedWallets: 150,
    transactions24h: 0,
    // would require on-chain indexer
    topReceivers: ["Endaoment", "The Giving Block", "UNICEF Crypto Fund", "Save the Children"],
    pctOfTotal: 0.8
    // ~0.8% of total charitable giving
  };
}
function getInstitutionalBaseline() {
  return {
    oecdOdaAnnualUsdBn: 223.7,
    // 2023 preliminary
    oecdDataYear: 2023,
    cafWorldGivingIndex: 34,
    // 2024 CAF World Giving Index (global avg %)
    cafDataYear: 2024,
    candidGrantsTracked: 18e6,
    // Candid tracks ~18M grants
    dataLag: "Annual"
  };
}
function getDefaultCategories() {
  return [
    { category: "Medical & Health", share: 0.33, change24h: 0, activeCampaigns: 0, trending: true },
    { category: "Disaster Relief", share: 0.15, change24h: 0, activeCampaigns: 0, trending: false },
    { category: "Education", share: 0.12, change24h: 0, activeCampaigns: 0, trending: false },
    { category: "Community", share: 0.1, change24h: 0, activeCampaigns: 0, trending: false },
    { category: "Memorials", share: 0.08, change24h: 0, activeCampaigns: 0, trending: false },
    { category: "Animals & Pets", share: 0.07, change24h: 0, activeCampaigns: 0, trending: false },
    { category: "Environment", share: 0.05, change24h: 0, activeCampaigns: 0, trending: false },
    { category: "Hunger & Food", share: 0.05, change24h: 0, activeCampaigns: 0, trending: false },
    { category: "Other", share: 0.05, change24h: 0, activeCampaigns: 0, trending: false }
  ];
}
function computeActivityIndex(platforms, crypto) {
  let score = 50;
  const totalDailyVolume = platforms.reduce((s, p) => s + p.dailyVolumeUsd, 0) + crypto.dailyInflowUsd;
  const volumeRatio = totalDailyVolume / 5e7;
  score += Math.min(20, Math.max(-20, (volumeRatio - 1) * 20));
  const totalVelocity = platforms.reduce((s, p) => s + p.donationVelocity, 0);
  if (totalVelocity > 100) score += 5;
  if (totalVelocity > 500) score += 10;
  const totalNew = platforms.reduce((s, p) => s + p.newCampaigns24h, 0);
  if (totalNew > 10) score += 5;
  if (totalNew > 50) score += 5;
  const reporting = platforms.filter((p) => p.dailyVolumeUsd > 0).length;
  score += reporting * 2;
  return Math.max(0, Math.min(100, Math.round(score)));
}
function computeTrend(index) {
  if (index >= 65) return "rising";
  if (index <= 35) return "falling";
  return "stable";
}
async function getGivingSummary(_ctx, req) {
  try {
    const result = await cachedFetchJson(REDIS_CACHE_KEY35, REDIS_CACHE_TTL35, async () => {
      const cryptoEstimate = getCryptoGivingEstimate();
      const gofundme = getGoFundMeEstimate();
      const globalGiving = getGlobalGivingEstimate();
      const justGiving = getJustGivingEstimate();
      const institutional = getInstitutionalBaseline();
      const platforms = [gofundme, globalGiving, justGiving];
      const categories = getDefaultCategories();
      const activityIndex = computeActivityIndex(platforms, cryptoEstimate);
      const trend = computeTrend(activityIndex);
      const estimatedDailyFlowUsd = platforms.reduce((s, p) => s + p.dailyVolumeUsd, 0) + cryptoEstimate.dailyInflowUsd;
      const summary2 = {
        generatedAt: (/* @__PURE__ */ new Date()).toISOString(),
        activityIndex,
        trend,
        estimatedDailyFlowUsd,
        platforms,
        categories,
        crypto: cryptoEstimate,
        institutional
      };
      return { summary: summary2 };
    });
    if (!result) return { summary: void 0 };
    const summary = result.summary;
    if (!summary) return { summary };
    return {
      summary: {
        ...summary,
        platforms: req.platformLimit > 0 && summary.platforms ? summary.platforms.slice(0, req.platformLimit) : summary.platforms,
        categories: req.categoryLimit > 0 && summary.categories ? summary.categories.slice(0, req.categoryLimit) : summary.categories
      }
    };
  } catch {
    return { summary: void 0 };
  }
}

// server/worldmonitor/giving/v1/handler.ts
var givingHandler = {
  getGivingSummary
};

// src/generated/server/worldmonitor/trade/v1/service_server.ts
var ValidationError20 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createTradeServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/trade/v1/get-trade-restrictions",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            countries: params.getAll("countries"),
            limit: Number(params.get("limit") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getTradeRestrictions", body);
            if (bodyViolations) {
              throw new ValidationError20(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getTradeRestrictions(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError20) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/trade/v1/get-tariff-trends",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            reportingCountry: params.get("reporting_country") ?? "",
            partnerCountry: params.get("partner_country") ?? "",
            productSector: params.get("product_sector") ?? "",
            years: Number(params.get("years") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getTariffTrends", body);
            if (bodyViolations) {
              throw new ValidationError20(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getTariffTrends(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError20) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/trade/v1/get-trade-flows",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            reportingCountry: params.get("reporting_country") ?? "",
            partnerCountry: params.get("partner_country") ?? "",
            years: Number(params.get("years") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getTradeFlows", body);
            if (bodyViolations) {
              throw new ValidationError20(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getTradeFlows(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError20) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/trade/v1/get-trade-barriers",
      handler: async (req) => {
        try {
          const pathParams = {};
          const url = new URL(req.url, "http://localhost");
          const params = url.searchParams;
          const body = {
            countries: params.getAll("countries"),
            measureType: params.get("measure_type") ?? "",
            limit: Number(params.get("limit") ?? "0")
          };
          if (options?.validateRequest) {
            const bodyViolations = options.validateRequest("getTradeBarriers", body);
            if (bodyViolations) {
              throw new ValidationError20(bodyViolations);
            }
          }
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getTradeBarriers(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError20) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/trade/v1/_shared.ts
var WTO_API_BASE = "https://api.wto.org/timeseries/v1";
var ITS_MTV_AX = "ITS_MTV_AX";
var ITS_MTV_AM = "ITS_MTV_AM";
var TP_A_0010 = "TP_A_0010";
var WTO_MEMBER_CODES = {
  "840": "United States",
  "156": "China",
  "276": "Germany",
  "392": "Japan",
  "826": "United Kingdom",
  "250": "France",
  "356": "India",
  "643": "Russia",
  "076": "Brazil",
  "410": "South Korea",
  "036": "Australia",
  "124": "Canada",
  "484": "Mexico",
  "380": "Italy",
  "528": "Netherlands",
  "000": "World"
};
async function wtoFetch(path, params) {
  const apiKey = process.env.WTO_API_KEY;
  if (!apiKey) {
    console.warn("[WTO] WTO_API_KEY not set in process.env");
    return null;
  }
  try {
    const url = new URL(`${WTO_API_BASE}${path}`);
    if (params) {
      for (const [k, v] of Object.entries(params)) {
        url.searchParams.set(k, v);
      }
    }
    const res = await fetch(url.toString(), {
      headers: {
        "Ocp-Apim-Subscription-Key": apiKey,
        "User-Agent": CHROME_UA
      },
      signal: AbortSignal.timeout(15e3)
    });
    if (res.status === 204) return { Dataset: [] };
    if (!res.ok) {
      console.warn(`[WTO] HTTP ${res.status} for ${path}`);
      return null;
    }
    return await res.json();
  } catch (e) {
    console.error("[WTO] Fetch error:", e instanceof Error ? e.message : e);
    return null;
  }
}

// server/worldmonitor/trade/v1/get-trade-restrictions.ts
var REDIS_CACHE_KEY36 = "trade:restrictions:v1";
var REDIS_CACHE_TTL36 = 21600;
var MAJOR_REPORTERS = ["840", "156", "276", "392", "826", "356", "076", "643", "410", "036", "124", "484", "250", "380", "528"];
function toRestriction(row) {
  if (!row) return null;
  const value = parseFloat(row.Value ?? row.value ?? "");
  if (isNaN(value)) return null;
  const reporterCode = String(row.ReportingEconomyCode ?? row.reportingEconomyCode ?? "");
  const year = String(row.Year ?? row.year ?? row.Period ?? "");
  const indicator = String(row.Indicator ?? row.indicator ?? row.IndicatorCode ?? "");
  return {
    id: `${reporterCode}-${year}-${row.IndicatorCode ?? ""}`,
    reportingCountry: WTO_MEMBER_CODES[reporterCode] ?? String(row.ReportingEconomy ?? row.reportingEconomy ?? ""),
    affectedCountry: "All trading partners",
    productSector: indicator.includes("agricultural") ? indicator.includes("non-") ? "Non-agricultural products" : "Agricultural products" : "All products",
    measureType: "MFN Applied Tariff",
    description: `Average tariff rate: ${value.toFixed(1)}%`,
    status: value > 10 ? "high" : value > 5 ? "moderate" : "low",
    notifiedAt: year,
    sourceUrl: "https://stats.wto.org"
  };
}
async function fetchRestrictions(_countries, limit) {
  const currentYear = (/* @__PURE__ */ new Date()).getFullYear();
  const params = {
    i: "TP_A_0010",
    r: MAJOR_REPORTERS.join(","),
    ps: `${currentYear - 3}-${currentYear}`,
    fmt: "json",
    mode: "full",
    max: String(limit * 3)
  };
  const data = await wtoFetch("/data", params);
  if (!data) return { restrictions: [], ok: false };
  const dataset = Array.isArray(data) ? data : data?.Dataset ?? data?.dataset ?? [];
  const latestByCountry = /* @__PURE__ */ new Map();
  for (const row of dataset) {
    const code = String(row.ReportingEconomyCode ?? "");
    const year = parseInt(row.Year ?? row.year ?? "0", 10);
    const existing = latestByCountry.get(code);
    if (!existing || year > parseInt(existing.Year ?? existing.year ?? "0", 10)) {
      latestByCountry.set(code, row);
    }
  }
  const restrictions = Array.from(latestByCountry.values()).map(toRestriction).filter((r) => r !== null).sort((a, b) => {
    const rateA = parseFloat(a.description.match(/[\d.]+/)?.[0] ?? "0");
    const rateB = parseFloat(b.description.match(/[\d.]+/)?.[0] ?? "0");
    return rateB - rateA;
  }).slice(0, limit);
  return { restrictions, ok: true };
}
async function getTradeRestrictions(_ctx, req) {
  try {
    const limit = Math.max(1, Math.min(req.limit > 0 ? req.limit : 50, 100));
    const cacheKey2 = `${REDIS_CACHE_KEY36}:tariff-overview:${limit}`;
    const result = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL36,
      async () => {
        const { restrictions, ok } = await fetchRestrictions([], limit);
        if (!ok || restrictions.length === 0) return null;
        return { restrictions, fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: false };
      }
    );
    return result ?? { restrictions: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  } catch {
    return {
      restrictions: [],
      fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
      upstreamUnavailable: true
    };
  }
}

// server/worldmonitor/trade/v1/get-tariff-trends.ts
var REDIS_CACHE_TTL37 = 21600;
function isValidCode(c) {
  return /^[a-zA-Z0-9]{1,10}$/.test(c);
}
function toDataPoint(row, reporter, partner) {
  if (!row) return null;
  const year = parseInt(row.Year ?? row.year ?? row.Period ?? "", 10);
  const tariffRate = parseFloat(row.Value ?? row.value ?? "");
  if (isNaN(year) || isNaN(tariffRate)) return null;
  return {
    reportingCountry: WTO_MEMBER_CODES[reporter] ?? String(row.ReportingEconomy ?? row.reportingEconomy ?? reporter),
    partnerCountry: WTO_MEMBER_CODES[partner] ?? String(row.PartnerEconomy ?? row.partnerEconomy ?? (partner || "World")),
    productSector: String(row.ProductOrSector ?? row.productOrSector ?? "All products"),
    year,
    tariffRate: Math.round(tariffRate * 100) / 100,
    boundRate: parseFloat(row.BoundRate ?? row.boundRate ?? "0") || 0,
    indicatorCode: String(row.IndicatorCode ?? row.indicatorCode ?? TP_A_0010)
  };
}
async function fetchTariffTrends(reporter, partner, _productSector, years) {
  const currentYear = (/* @__PURE__ */ new Date()).getFullYear();
  const startYear = currentYear - years;
  const params = {
    i: TP_A_0010,
    r: reporter,
    ps: `${startYear}-${currentYear}`,
    fmt: "json",
    mode: "full",
    max: "500"
  };
  const data = await wtoFetch("/data", params);
  if (!data) return { datapoints: [], ok: false };
  const dataset = Array.isArray(data) ? data : data?.Dataset ?? data?.dataset ?? [];
  const datapoints = dataset.map((row) => toDataPoint(row, reporter, partner || "000")).filter((d) => d !== null).sort((a, b) => a.year - b.year);
  return { datapoints, ok: true };
}
async function getTariffTrends(_ctx, req) {
  try {
    const reporter = isValidCode(req.reportingCountry) ? req.reportingCountry : "840";
    const partner = isValidCode(req.partnerCountry) ? req.partnerCountry : "000";
    const productSector = isValidCode(req.productSector) ? req.productSector : "";
    const years = Math.max(1, Math.min(req.years > 0 ? req.years : 10, 30));
    const cacheKey2 = `trade:tariffs:v1:${reporter}:${productSector || "all"}:${years}`;
    const result = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL37,
      async () => {
        const { datapoints, ok } = await fetchTariffTrends(reporter, partner, productSector, years);
        if (!ok || datapoints.length === 0) return null;
        return { datapoints, fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: false };
      }
    );
    return result ?? { datapoints: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  } catch {
    return {
      datapoints: [],
      fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
      upstreamUnavailable: true
    };
  }
}

// server/worldmonitor/trade/v1/get-trade-flows.ts
var REDIS_CACHE_TTL38 = 21600;
function isValidCode2(c) {
  return /^[a-zA-Z0-9]{1,10}$/.test(c);
}
function parseRows(data, indicator) {
  const dataset = Array.isArray(data) ? data : data?.Dataset ?? data?.dataset ?? [];
  const rows = [];
  for (const row of dataset) {
    const year = parseInt(row.Year ?? row.year ?? row.Period ?? "", 10);
    const value = parseFloat(row.Value ?? row.value ?? "");
    if (!isNaN(year) && !isNaN(value)) {
      rows.push({ year, indicator, value });
    }
  }
  return rows;
}
function buildFlowRecords(rows, reporter, partner) {
  const byYear = /* @__PURE__ */ new Map();
  for (const row of rows) {
    if (!byYear.has(row.year)) {
      byYear.set(row.year, { exports: 0, imports: 0 });
    }
    const entry = byYear.get(row.year);
    if (row.indicator === ITS_MTV_AX) {
      entry.exports = row.value;
    } else if (row.indicator === ITS_MTV_AM) {
      entry.imports = row.value;
    }
  }
  const sortedYears = Array.from(byYear.keys()).sort((a, b) => a - b);
  const records = [];
  for (let i = 0; i < sortedYears.length; i++) {
    const year = sortedYears[i];
    const current = byYear.get(year);
    const prev = i > 0 ? byYear.get(sortedYears[i - 1]) : null;
    let yoyExportChange = 0;
    let yoyImportChange = 0;
    if (prev && prev.exports > 0) {
      yoyExportChange = Math.round((current.exports - prev.exports) / prev.exports * 1e4) / 100;
    }
    if (prev && prev.imports > 0) {
      yoyImportChange = Math.round((current.imports - prev.imports) / prev.imports * 1e4) / 100;
    }
    records.push({
      reportingCountry: WTO_MEMBER_CODES[reporter] ?? reporter,
      partnerCountry: WTO_MEMBER_CODES[partner] ?? partner,
      year,
      exportValueUsd: current.exports,
      importValueUsd: current.imports,
      yoyExportChange,
      yoyImportChange,
      productSector: "Total merchandise"
    });
  }
  return records;
}
async function fetchTradeFlows(reporter, partner, years) {
  const currentYear = (/* @__PURE__ */ new Date()).getFullYear();
  const startYear = currentYear - years;
  const baseParams = {
    r: reporter,
    p: partner || "000",
    ps: `${startYear}-${currentYear}`,
    pc: "TO",
    fmt: "json",
    mode: "full",
    max: "500"
  };
  const [exportsData, importsData] = await Promise.all([
    wtoFetch("/data", { ...baseParams, i: ITS_MTV_AX }),
    wtoFetch("/data", { ...baseParams, i: ITS_MTV_AM })
  ]);
  if (!exportsData && !importsData) return { flows: [], ok: false };
  const rows = [
    ...exportsData ? parseRows(exportsData, ITS_MTV_AX) : [],
    ...importsData ? parseRows(importsData, ITS_MTV_AM) : []
  ];
  const flows = buildFlowRecords(rows, reporter, partner || "000");
  return { flows, ok: true };
}
async function getTradeFlows(_ctx, req) {
  try {
    const reporter = isValidCode2(req.reportingCountry) ? req.reportingCountry : "840";
    const partner = isValidCode2(req.partnerCountry) ? req.partnerCountry : "000";
    const years = Math.max(1, Math.min(req.years > 0 ? req.years : 10, 30));
    const cacheKey2 = `trade:flows:v1:${reporter}:${partner}:${years}`;
    const result = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL38,
      async () => {
        const { flows, ok } = await fetchTradeFlows(reporter, partner, years);
        if (!ok || flows.length === 0) return null;
        return { flows, fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: false };
      }
    );
    return result ?? { flows: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  } catch {
    return {
      flows: [],
      fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
      upstreamUnavailable: true
    };
  }
}

// server/worldmonitor/trade/v1/get-trade-barriers.ts
var REDIS_CACHE_TTL39 = 21600;
var MAJOR_REPORTERS2 = ["840", "156", "276", "392", "826", "356", "076", "643", "410", "036", "124", "484", "250", "380", "528"];
function isValidCountry(c) {
  return /^[a-zA-Z0-9]{1,10}$/.test(c);
}
function parseRows2(data) {
  const dataset = Array.isArray(data) ? data : data?.Dataset ?? data?.dataset ?? [];
  const rows = [];
  for (const row of dataset) {
    const year = parseInt(row.Year ?? row.year ?? "0", 10);
    const value = parseFloat(row.Value ?? row.value ?? "");
    if (isNaN(year) || isNaN(value)) continue;
    const countryCode = String(row.ReportingEconomyCode ?? "");
    rows.push({
      country: WTO_MEMBER_CODES[countryCode] ?? String(row.ReportingEconomy ?? ""),
      countryCode,
      indicator: String(row.IndicatorCode ?? ""),
      year,
      value
    });
  }
  return rows;
}
async function fetchBarriers(_countries, limit) {
  const currentYear = (/* @__PURE__ */ new Date()).getFullYear();
  const reporters = MAJOR_REPORTERS2.join(",");
  const [agriData, nonAgriData] = await Promise.all([
    wtoFetch("/data", {
      i: "TP_A_0160",
      r: reporters,
      ps: `${currentYear - 3}-${currentYear}`,
      fmt: "json",
      mode: "full",
      max: "500"
    }),
    wtoFetch("/data", {
      i: "TP_A_0430",
      r: reporters,
      ps: `${currentYear - 3}-${currentYear}`,
      fmt: "json",
      mode: "full",
      max: "500"
    })
  ]);
  if (!agriData && !nonAgriData) return { barriers: [], ok: false };
  const agriRows = agriData ? parseRows2(agriData) : [];
  const nonAgriRows = nonAgriData ? parseRows2(nonAgriData) : [];
  const latestAgri = /* @__PURE__ */ new Map();
  for (const row of agriRows) {
    const existing = latestAgri.get(row.countryCode);
    if (!existing || row.year > existing.year) {
      latestAgri.set(row.countryCode, row);
    }
  }
  const latestNonAgri = /* @__PURE__ */ new Map();
  for (const row of nonAgriRows) {
    const existing = latestNonAgri.get(row.countryCode);
    if (!existing || row.year > existing.year) {
      latestNonAgri.set(row.countryCode, row);
    }
  }
  const barriers = [];
  const allCodes = /* @__PURE__ */ new Set([...latestAgri.keys(), ...latestNonAgri.keys()]);
  for (const code of allCodes) {
    const agri = latestAgri.get(code);
    const nonAgri = latestNonAgri.get(code);
    if (!agri && !nonAgri) continue;
    const agriRate = agri?.value ?? 0;
    const nonAgriRate = nonAgri?.value ?? 0;
    const gap = agriRate - nonAgriRate;
    const country = agri?.country ?? nonAgri?.country ?? code;
    const year = String(agri?.year ?? nonAgri?.year ?? "");
    barriers.push({
      id: `${code}-tariff-gap-${year}`,
      notifyingCountry: country,
      title: `Agricultural tariff: ${agriRate.toFixed(1)}% vs Non-agricultural: ${nonAgriRate.toFixed(1)}% (gap: ${gap > 0 ? "+" : ""}${gap.toFixed(1)}pp)`,
      measureType: gap > 10 ? "High agricultural protection" : gap > 5 ? "Moderate agricultural protection" : "Low tariff gap",
      productDescription: "Agricultural vs Non-agricultural products",
      objective: gap > 0 ? "Agricultural sector protection" : "Uniform tariff structure",
      status: gap > 10 ? "high" : gap > 5 ? "moderate" : "low",
      dateDistributed: year,
      sourceUrl: "https://stats.wto.org"
    });
  }
  barriers.sort((a, b) => {
    const gapA = parseFloat(a.title.match(/gap: ([+-]?\d+\.?\d*)/)?.[1] ?? "0");
    const gapB = parseFloat(b.title.match(/gap: ([+-]?\d+\.?\d*)/)?.[1] ?? "0");
    return gapB - gapA;
  });
  return { barriers: barriers.slice(0, limit), ok: true };
}
async function getTradeBarriers(_ctx, req) {
  try {
    const countries = (req.countries ?? []).filter(isValidCountry);
    const limit = Math.max(1, Math.min(req.limit > 0 ? req.limit : 50, 100));
    const cacheKey2 = `trade:barriers:v1:tariff-gap:${limit}`;
    const result = await cachedFetchJson(
      cacheKey2,
      REDIS_CACHE_TTL39,
      async () => {
        const { barriers, ok } = await fetchBarriers(countries, limit);
        if (!ok || barriers.length === 0) return null;
        return { barriers, fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: false };
      }
    );
    return result ?? { barriers: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  } catch {
    return {
      barriers: [],
      fetchedAt: (/* @__PURE__ */ new Date()).toISOString(),
      upstreamUnavailable: true
    };
  }
}

// server/worldmonitor/trade/v1/handler.ts
var tradeHandler = {
  getTradeRestrictions,
  getTariffTrends,
  getTradeFlows,
  getTradeBarriers
};

// src/generated/server/worldmonitor/supply_chain/v1/service_server.ts
var ValidationError21 = class extends Error {
  violations;
  constructor(violations) {
    super("Validation failed");
    this.name = "ValidationError";
    this.violations = violations;
  }
};
function createSupplyChainServiceRoutes(handler2, options) {
  return [
    {
      method: "GET",
      path: "/api/supply-chain/v1/get-shipping-rates",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getShippingRates(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError21) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/supply-chain/v1/get-chokepoint-status",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getChokepointStatus(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError21) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    },
    {
      method: "GET",
      path: "/api/supply-chain/v1/get-critical-minerals",
      handler: async (req) => {
        try {
          const pathParams = {};
          const body = {};
          const ctx = {
            request: req,
            pathParams,
            headers: Object.fromEntries(req.headers.entries())
          };
          const result = await handler2.getCriticalMinerals(ctx, body);
          return new Response(JSON.stringify(result), {
            status: 200,
            headers: { "Content-Type": "application/json" }
          });
        } catch (err) {
          if (err instanceof ValidationError21) {
            return new Response(JSON.stringify({ violations: err.violations }), {
              status: 400,
              headers: { "Content-Type": "application/json" }
            });
          }
          if (options?.onError) {
            return options.onError(err, req);
          }
          const message = err instanceof Error ? err.message : String(err);
          return new Response(JSON.stringify({ message }), {
            status: 500,
            headers: { "Content-Type": "application/json" }
          });
        }
      }
    }
  ];
}

// server/worldmonitor/supply-chain/v1/_scoring.mjs
var SEVERITY_SCORE = {
  "AIS_DISRUPTION_SEVERITY_LOW": 1,
  "AIS_DISRUPTION_SEVERITY_ELEVATED": 2,
  "AIS_DISRUPTION_SEVERITY_HIGH": 3
};
function computeDisruptionScore(warningCount, congestionSeverity) {
  return Math.min(100, warningCount * 15 + congestionSeverity * 30);
}
function scoreToStatus(score) {
  if (score < 20) return "green";
  if (score < 50) return "yellow";
  return "red";
}
function computeHHI(shares) {
  if (!shares || shares.length === 0) return 0;
  return shares.reduce((sum, s) => sum + s * s, 0);
}
function riskRating(hhi) {
  if (hhi >= 5e3) return "critical";
  if (hhi >= 2500) return "high";
  if (hhi >= 1500) return "moderate";
  return "low";
}
function detectSpike(history) {
  if (!history || history.length < 3) return false;
  const values = history.map((h) => typeof h === "number" ? h : h.value).filter((v) => Number.isFinite(v));
  if (values.length < 3) return false;
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const variance = values.reduce((sum, v) => sum + (v - mean) ** 2, 0) / values.length;
  const stdDev = Math.sqrt(variance);
  if (stdDev === 0) return false;
  const latest = values[values.length - 1];
  return latest > mean + 2 * stdDev;
}

// server/worldmonitor/supply-chain/v1/get-shipping-rates.ts
var FRED_API_BASE2 = "https://api.stlouisfed.org/fred";
var REDIS_CACHE_KEY37 = "supply_chain:shipping:v2";
var REDIS_CACHE_TTL40 = 3600;
var SHIPPING_SERIES = [
  { seriesId: "PCU483111483111", name: "Deep Sea Freight PPI", unit: "index", frequency: "m" },
  { seriesId: "TSIFRGHT", name: "Freight Transportation Index", unit: "index", frequency: "m" }
];
async function fetchFredSeries2(cfg) {
  const apiKey = process.env.FRED_API_KEY;
  if (!apiKey) return null;
  try {
    const params = new URLSearchParams({
      series_id: cfg.seriesId,
      api_key: apiKey,
      file_type: "json",
      frequency: cfg.frequency,
      sort_order: "desc",
      limit: "24"
    });
    const response = await fetch(`${FRED_API_BASE2}/series/observations?${params}`, {
      headers: { Accept: "application/json", "User-Agent": CHROME_UA },
      signal: AbortSignal.timeout(1e4)
    });
    if (!response.ok) return null;
    const data = await response.json();
    const observations = (data.observations || []).map((obs) => {
      const value = parseFloat(obs.value);
      if (isNaN(value) || obs.value === ".") return null;
      return { date: obs.date, value };
    }).filter((o) => o !== null).reverse();
    if (observations.length === 0) return null;
    const currentValue = observations[observations.length - 1].value;
    const previousValue = observations.length > 1 ? observations[observations.length - 2].value : currentValue;
    const changePct = previousValue !== 0 ? (currentValue - previousValue) / previousValue * 100 : 0;
    const spikeAlert = detectSpike(observations);
    return {
      indexId: cfg.seriesId,
      name: cfg.name,
      currentValue,
      previousValue,
      changePct,
      unit: cfg.unit,
      history: observations,
      spikeAlert
    };
  } catch {
    return null;
  }
}
async function getShippingRates(_ctx, _req) {
  try {
    const result = await cachedFetchJson(
      REDIS_CACHE_KEY37,
      REDIS_CACHE_TTL40,
      async () => {
        const results = await Promise.all(SHIPPING_SERIES.map(fetchFredSeries2));
        const indices = results.filter((r) => r !== null);
        if (indices.length === 0) return null;
        return { indices, fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: false };
      }
    );
    return result ?? { indices: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  } catch {
    return { indices: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  }
}

// server/worldmonitor/supply-chain/v1/get-chokepoint-status.ts
var REDIS_CACHE_KEY38 = "supply_chain:chokepoints:v1";
var REDIS_CACHE_TTL41 = 900;
var CHOKEPOINTS = [
  { id: "suez", name: "Suez Canal", lat: 30.45, lon: 32.35, areaKeywords: ["suez", "red sea"], routes: ["China-Europe (Suez)", "Gulf-Europe Oil", "Qatar LNG-Europe"] },
  { id: "malacca", name: "Malacca Strait", lat: 1.43, lon: 103.5, areaKeywords: ["malacca", "singapore strait"], routes: ["China-Middle East Oil", "China-Europe (via Suez)", "Japan-Middle East Oil"] },
  { id: "hormuz", name: "Strait of Hormuz", lat: 26.56, lon: 56.25, areaKeywords: ["hormuz", "persian gulf", "arabian gulf"], routes: ["Gulf Oil Exports", "Qatar LNG", "Iran Exports"] },
  { id: "bab_el_mandeb", name: "Bab el-Mandeb", lat: 12.58, lon: 43.33, areaKeywords: ["bab el-mandeb", "bab al-mandab", "mandeb", "aden"], routes: ["Suez-Indian Ocean", "Gulf-Europe Oil", "Red Sea Transit"] },
  { id: "panama", name: "Panama Canal", lat: 9.08, lon: -79.68, areaKeywords: ["panama"], routes: ["US East Coast-Asia", "US East Coast-South America", "Atlantic-Pacific Bulk"] },
  { id: "taiwan", name: "Taiwan Strait", lat: 24, lon: 119.5, areaKeywords: ["taiwan strait", "formosa"], routes: ["China-Japan Trade", "Korea-Southeast Asia", "Pacific Semiconductor"] }
];
function makeInternalCtx() {
  return { request: new Request("http://internal"), pathParams: {}, headers: {} };
}
async function fetchChokepointData() {
  const ctx = makeInternalCtx();
  let navFailed = false;
  let vesselFailed = false;
  const [navResult, vesselResult] = await Promise.all([
    listNavigationalWarnings(ctx, { area: "", pageSize: 0, cursor: "" }).catch(() => {
      navFailed = true;
      return { warnings: [], pagination: void 0 };
    }),
    getVesselSnapshot(ctx, { neLat: 90, neLon: 180, swLat: -90, swLon: -180 }).catch(() => {
      vesselFailed = true;
      return { snapshot: void 0 };
    })
  ]);
  const warnings = navResult.warnings || [];
  const disruptions = vesselResult.snapshot?.disruptions || [];
  const upstreamUnavailable = navFailed && vesselFailed || navFailed && disruptions.length === 0 || vesselFailed && warnings.length === 0;
  const chokepoints = CHOKEPOINTS.map((cp) => {
    const matchedWarnings = warnings.filter(
      (w) => cp.areaKeywords.some((kw) => w.text.toLowerCase().includes(kw) || w.area.toLowerCase().includes(kw))
    );
    const matchedDisruptions = disruptions.filter(
      (d) => d.type === "AIS_DISRUPTION_TYPE_CHOKEPOINT_CONGESTION" && cp.areaKeywords.some((kw) => d.region.toLowerCase().includes(kw) || d.name.toLowerCase().includes(kw))
    );
    const maxSeverity = matchedDisruptions.reduce((max, d) => {
      const score = SEVERITY_SCORE[d.severity] ?? 0;
      return Math.max(max, score);
    }, 0);
    const disruptionScore = computeDisruptionScore(matchedWarnings.length, maxSeverity);
    const status = scoreToStatus(disruptionScore);
    const congestionLevel = maxSeverity >= 3 ? "high" : maxSeverity >= 2 ? "elevated" : maxSeverity >= 1 ? "low" : "normal";
    const descriptions = [];
    if (matchedWarnings.length > 0) descriptions.push(`${matchedWarnings.length} active navigational warning(s)`);
    if (matchedDisruptions.length > 0) descriptions.push(`AIS congestion detected`);
    if (descriptions.length === 0) descriptions.push("No active disruptions");
    return {
      id: cp.id,
      name: cp.name,
      lat: cp.lat,
      lon: cp.lon,
      disruptionScore,
      status,
      activeWarnings: matchedWarnings.length,
      congestionLevel,
      affectedRoutes: cp.routes,
      description: descriptions.join("; ")
    };
  });
  return { chokepoints, upstreamUnavailable };
}
async function getChokepointStatus(_ctx, _req) {
  try {
    const result = await cachedFetchJson(
      REDIS_CACHE_KEY38,
      REDIS_CACHE_TTL41,
      async () => {
        const { chokepoints, upstreamUnavailable } = await fetchChokepointData();
        if (upstreamUnavailable) return null;
        return { chokepoints, fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable };
      }
    );
    return result ?? { chokepoints: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  } catch {
    return { chokepoints: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  }
}

// server/worldmonitor/supply-chain/v1/_minerals-data.ts
var MINERAL_PRODUCTION_2024 = [
  // Lithium (tonnes LCE)
  { mineral: "Lithium", country: "Australia", countryCode: "AU", productionTonnes: 86e3, unit: "tonnes LCE" },
  { mineral: "Lithium", country: "Chile", countryCode: "CL", productionTonnes: 44e3, unit: "tonnes LCE" },
  { mineral: "Lithium", country: "China", countryCode: "CN", productionTonnes: 33e3, unit: "tonnes LCE" },
  { mineral: "Lithium", country: "Argentina", countryCode: "AR", productionTonnes: 9600, unit: "tonnes LCE" },
  // Cobalt (tonnes)
  { mineral: "Cobalt", country: "DRC", countryCode: "CD", productionTonnes: 13e4, unit: "tonnes" },
  { mineral: "Cobalt", country: "Indonesia", countryCode: "ID", productionTonnes: 17e3, unit: "tonnes" },
  { mineral: "Cobalt", country: "Russia", countryCode: "RU", productionTonnes: 8900, unit: "tonnes" },
  { mineral: "Cobalt", country: "Australia", countryCode: "AU", productionTonnes: 5600, unit: "tonnes" },
  // Rare Earths (tonnes REO)
  { mineral: "Rare Earths", country: "China", countryCode: "CN", productionTonnes: 24e4, unit: "tonnes REO" },
  { mineral: "Rare Earths", country: "Myanmar", countryCode: "MM", productionTonnes: 38e3, unit: "tonnes REO" },
  { mineral: "Rare Earths", country: "USA", countryCode: "US", productionTonnes: 43e3, unit: "tonnes REO" },
  { mineral: "Rare Earths", country: "Australia", countryCode: "AU", productionTonnes: 18e3, unit: "tonnes REO" },
  // Nickel (tonnes)
  { mineral: "Nickel", country: "Indonesia", countryCode: "ID", productionTonnes: 18e5, unit: "tonnes" },
  { mineral: "Nickel", country: "Philippines", countryCode: "PH", productionTonnes: 33e4, unit: "tonnes" },
  { mineral: "Nickel", country: "Russia", countryCode: "RU", productionTonnes: 22e4, unit: "tonnes" },
  { mineral: "Nickel", country: "New Caledonia", countryCode: "NC", productionTonnes: 19e4, unit: "tonnes" },
  // Copper (tonnes)
  { mineral: "Copper", country: "Chile", countryCode: "CL", productionTonnes: 53e5, unit: "tonnes" },
  { mineral: "Copper", country: "DRC", countryCode: "CD", productionTonnes: 25e5, unit: "tonnes" },
  { mineral: "Copper", country: "Peru", countryCode: "PE", productionTonnes: 22e5, unit: "tonnes" },
  { mineral: "Copper", country: "China", countryCode: "CN", productionTonnes: 19e5, unit: "tonnes" },
  // Gallium (tonnes)
  { mineral: "Gallium", country: "China", countryCode: "CN", productionTonnes: 600, unit: "tonnes" },
  { mineral: "Gallium", country: "Japan", countryCode: "JP", productionTonnes: 10, unit: "tonnes" },
  { mineral: "Gallium", country: "South Korea", countryCode: "KR", productionTonnes: 8, unit: "tonnes" },
  { mineral: "Gallium", country: "Russia", countryCode: "RU", productionTonnes: 5, unit: "tonnes" },
  // Germanium (tonnes)
  { mineral: "Germanium", country: "China", countryCode: "CN", productionTonnes: 95, unit: "tonnes" },
  { mineral: "Germanium", country: "Belgium", countryCode: "BE", productionTonnes: 15, unit: "tonnes" },
  { mineral: "Germanium", country: "Canada", countryCode: "CA", productionTonnes: 9, unit: "tonnes" },
  { mineral: "Germanium", country: "Russia", countryCode: "RU", productionTonnes: 5, unit: "tonnes" }
];

// server/worldmonitor/supply-chain/v1/get-critical-minerals.ts
var REDIS_CACHE_KEY39 = "supply_chain:minerals:v1";
var REDIS_CACHE_TTL42 = 86400;
function buildMineralsData() {
  const byMineral = /* @__PURE__ */ new Map();
  for (const entry of MINERAL_PRODUCTION_2024) {
    const existing = byMineral.get(entry.mineral) || [];
    existing.push(entry);
    byMineral.set(entry.mineral, existing);
  }
  const minerals = [];
  for (const [mineral, entries] of byMineral) {
    const globalProduction = entries.reduce((sum, e) => sum + e.productionTonnes, 0);
    const unit = entries[0]?.unit || "tonnes";
    const producers = entries.sort((a, b) => b.productionTonnes - a.productionTonnes).slice(0, 5).map((e) => ({
      country: e.country,
      countryCode: e.countryCode,
      productionTonnes: e.productionTonnes,
      sharePct: globalProduction > 0 ? e.productionTonnes / globalProduction * 100 : 0
    }));
    const shares = entries.map((e) => globalProduction > 0 ? e.productionTonnes / globalProduction * 100 : 0);
    const hhi = computeHHI(shares);
    minerals.push({
      mineral,
      topProducers: producers,
      hhi,
      riskRating: riskRating(hhi),
      globalProduction,
      unit
    });
  }
  return minerals.sort((a, b) => b.hhi - a.hhi);
}
async function getCriticalMinerals(_ctx, _req) {
  try {
    const result = await cachedFetchJson(
      REDIS_CACHE_KEY39,
      REDIS_CACHE_TTL42,
      async () => {
        const minerals = buildMineralsData();
        return { minerals, fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: false };
      }
    );
    return result ?? { minerals: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  } catch {
    return { minerals: [], fetchedAt: (/* @__PURE__ */ new Date()).toISOString(), upstreamUnavailable: true };
  }
}

// server/worldmonitor/supply-chain/v1/handler.ts
var supplyChainHandler = {
  getShippingRates,
  getChokepointStatus,
  getCriticalMinerals
};

// api/[domain]/v1/[rpc].ts
var config = { runtime: "edge" };
var TIER_HEADERS = {
  fast: "public, s-maxage=120, stale-while-revalidate=30, stale-if-error=300",
  medium: "public, s-maxage=300, stale-while-revalidate=60, stale-if-error=600",
  slow: "public, s-maxage=900, stale-while-revalidate=120, stale-if-error=1800",
  static: "public, s-maxage=3600, stale-while-revalidate=300, stale-if-error=7200",
  "no-store": "no-store"
};
var RPC_CACHE_TIER = {
  "/api/maritime/v1/get-vessel-snapshot": "no-store",
  "/api/market/v1/list-market-quotes": "medium",
  "/api/market/v1/list-crypto-quotes": "medium",
  "/api/market/v1/list-commodity-quotes": "medium",
  "/api/market/v1/list-stablecoin-markets": "medium",
  "/api/market/v1/get-sector-summary": "medium",
  "/api/market/v1/list-gulf-quotes": "medium",
  "/api/infrastructure/v1/list-service-statuses": "slow",
  "/api/seismology/v1/list-earthquakes": "slow",
  "/api/infrastructure/v1/list-internet-outages": "slow",
  "/api/unrest/v1/list-unrest-events": "slow",
  "/api/cyber/v1/list-cyber-threats": "slow",
  "/api/conflict/v1/list-acled-events": "slow",
  "/api/military/v1/get-theater-posture": "slow",
  "/api/infrastructure/v1/get-temporal-baseline": "slow",
  "/api/aviation/v1/list-airport-delays": "static",
  "/api/market/v1/get-country-stock-index": "slow",
  "/api/wildfire/v1/list-fire-detections": "static",
  "/api/maritime/v1/list-navigational-warnings": "static",
  "/api/supply-chain/v1/get-shipping-rates": "static",
  "/api/economic/v1/get-fred-series": "static",
  "/api/economic/v1/get-energy-prices": "static",
  "/api/research/v1/list-arxiv-papers": "static",
  "/api/research/v1/list-trending-repos": "static",
  "/api/giving/v1/get-giving-summary": "static",
  "/api/intelligence/v1/get-country-intel-brief": "static",
  "/api/climate/v1/list-climate-anomalies": "static",
  "/api/research/v1/list-tech-events": "static",
  "/api/military/v1/get-usni-fleet-report": "static",
  "/api/conflict/v1/list-ucdp-events": "static",
  "/api/conflict/v1/get-humanitarian-summary": "static",
  "/api/displacement/v1/get-displacement-summary": "static",
  "/api/displacement/v1/get-population-exposure": "static",
  "/api/economic/v1/get-bis-policy-rates": "static",
  "/api/economic/v1/get-bis-exchange-rates": "static",
  "/api/economic/v1/get-bis-credit": "static",
  "/api/trade/v1/get-tariff-trends": "static",
  "/api/trade/v1/get-trade-flows": "static",
  "/api/trade/v1/get-trade-barriers": "static",
  "/api/trade/v1/get-trade-restrictions": "static",
  "/api/economic/v1/list-world-bank-indicators": "static",
  "/api/economic/v1/get-energy-capacity": "static",
  "/api/supply-chain/v1/get-critical-minerals": "static",
  "/api/military/v1/get-aircraft-details": "static",
  "/api/military/v1/get-wingbits-status": "static",
  "/api/military/v1/list-military-flights": "slow",
  "/api/market/v1/list-etf-flows": "slow",
  "/api/research/v1/list-hackernews-items": "slow",
  "/api/intelligence/v1/get-risk-scores": "slow",
  "/api/intelligence/v1/get-pizzint-status": "slow",
  "/api/intelligence/v1/search-gdelt-documents": "slow",
  "/api/infrastructure/v1/get-cable-health": "slow",
  "/api/positive-events/v1/list-positive-geo-events": "slow",
  "/api/military/v1/list-military-bases": "static",
  "/api/economic/v1/get-macro-signals": "medium",
  "/api/prediction/v1/list-prediction-markets": "medium",
  "/api/supply-chain/v1/get-chokepoint-status": "medium",
  "/api/news/v1/list-feed-digest": "slow"
};
var serverOptions = { onError: mapErrorToResponse };
var allRoutes = [
  ...createSeismologyServiceRoutes(seismologyHandler, serverOptions),
  ...createWildfireServiceRoutes(wildfireHandler, serverOptions),
  ...createClimateServiceRoutes(climateHandler, serverOptions),
  ...createPredictionServiceRoutes(predictionHandler, serverOptions),
  ...createDisplacementServiceRoutes(displacementHandler, serverOptions),
  ...createAviationServiceRoutes(aviationHandler, serverOptions),
  ...createResearchServiceRoutes(researchHandler, serverOptions),
  ...createUnrestServiceRoutes(unrestHandler, serverOptions),
  ...createConflictServiceRoutes(conflictHandler, serverOptions),
  ...createMaritimeServiceRoutes(maritimeHandler, serverOptions),
  ...createCyberServiceRoutes(cyberHandler, serverOptions),
  ...createEconomicServiceRoutes(economicHandler, serverOptions),
  ...createInfrastructureServiceRoutes(infrastructureHandler, serverOptions),
  ...createMarketServiceRoutes(marketHandler, serverOptions),
  ...createNewsServiceRoutes(newsHandler, serverOptions),
  ...createIntelligenceServiceRoutes(intelligenceHandler, serverOptions),
  ...createMilitaryServiceRoutes(militaryHandler, serverOptions),
  ...createPositiveEventsServiceRoutes(positiveEventsHandler, serverOptions),
  ...createGivingServiceRoutes(givingHandler, serverOptions),
  ...createTradeServiceRoutes(tradeHandler, serverOptions),
  ...createSupplyChainServiceRoutes(supplyChainHandler, serverOptions)
];
var router = createRouter(allRoutes);
async function handler(originalRequest) {
  let request = originalRequest;
  if (isDisallowedOrigin(request)) {
    return new Response(JSON.stringify({ error: "Origin not allowed" }), {
      status: 403,
      headers: { "Content-Type": "application/json" }
    });
  }
  let corsHeaders;
  try {
    corsHeaders = getCorsHeaders(request);
  } catch {
    corsHeaders = { "Access-Control-Allow-Origin": "*" };
  }
  if (request.method === "OPTIONS") {
    return new Response(null, { status: 204, headers: corsHeaders });
  }
  const keyCheck = validateApiKey(request);
  if (keyCheck.required && !keyCheck.valid) {
    return new Response(JSON.stringify({ error: keyCheck.error }), {
      status: 401,
      headers: { "Content-Type": "application/json", ...corsHeaders }
    });
  }
  const rateLimitResponse = await checkRateLimit(request, corsHeaders);
  if (rateLimitResponse) return rateLimitResponse;
  let matchedHandler = router.match(request);
  if (!matchedHandler && request.method === "POST") {
    const contentLen = parseInt(request.headers.get("Content-Length") ?? "0", 10);
    if (contentLen < 1048576) {
      const url = new URL(request.url);
      try {
        const body = await request.clone().json();
        const isScalar = (x) => typeof x === "string" || typeof x === "number" || typeof x === "boolean";
        for (const [k, v] of Object.entries(body)) {
          if (Array.isArray(v)) v.forEach((item) => {
            if (isScalar(item)) url.searchParams.append(k, String(item));
          });
          else if (isScalar(v)) url.searchParams.set(k, String(v));
        }
      } catch {
      }
      const getReq = new Request(url.toString(), { method: "GET", headers: request.headers });
      matchedHandler = router.match(getReq);
      if (matchedHandler) request = getReq;
    }
  }
  if (!matchedHandler) {
    return new Response(JSON.stringify({ error: "Not found" }), {
      status: 404,
      headers: { "Content-Type": "application/json", ...corsHeaders }
    });
  }
  let response;
  try {
    response = await matchedHandler(request);
  } catch (err) {
    console.error("[gateway] Unhandled handler error:", err);
    response = new Response(JSON.stringify({ message: "Internal server error" }), {
      status: 500,
      headers: { "Content-Type": "application/json" }
    });
  }
  const mergedHeaders = new Headers(response.headers);
  for (const [key, value] of Object.entries(corsHeaders)) {
    mergedHeaders.set(key, value);
  }
  const extraHeaders = drainResponseHeaders(request);
  if (extraHeaders) {
    for (const [key, value] of Object.entries(extraHeaders)) {
      mergedHeaders.set(key, value);
    }
  }
  if (response.status === 200 && request.method === "GET" && !mergedHeaders.has("Cache-Control")) {
    if (mergedHeaders.get("X-No-Cache")) {
      mergedHeaders.set("Cache-Control", "no-store");
      mergedHeaders.set("X-Cache-Tier", "no-store");
    } else {
      const pathname = new URL(request.url).pathname;
      const rpcName = pathname.split("/").pop() ?? "";
      const envOverride = process.env[`CACHE_TIER_OVERRIDE_${rpcName.replace(/-/g, "_").toUpperCase()}`];
      const tier = (envOverride && envOverride in TIER_HEADERS ? envOverride : null) ?? RPC_CACHE_TIER[pathname] ?? "medium";
      mergedHeaders.set("Cache-Control", TIER_HEADERS[tier]);
      mergedHeaders.set("X-Cache-Tier", tier);
    }
  }
  mergedHeaders.delete("X-No-Cache");
  if (!new URL(request.url).searchParams.has("_debug")) {
    mergedHeaders.delete("X-Cache-Tier");
  }
  return new Response(response.body, {
    status: response.status,
    statusText: response.statusText,
    headers: mergedHeaders
  });
}
export {
  config,
  handler as default
};
/*! Bundled license information:

papaparse/papaparse.js:
  (* @license
  Papa Parse
  v5.5.3
  https://github.com/mholt/PapaParse
  License: MIT
  *)
*/
