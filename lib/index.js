
/*
 * This is mostly based on the OpenTracing semantic spec
 * (http://opentracing.io/spec/), but importantly:
 *
 *  * Does not support "Baggage" items.
 *  * There's no "finish()" for Spans, instead use .log(..., true) to finish
 *    and that's only actually going to add 'end' when you created the span.
 *
 * It is also focussed on being used by Joyent's SDC tools so it has helpers
 * that tie in with Restify and also for attaching data to API clients so that
 * those clients' requests can be automatically traced.
 *
 * To add a tracer which will log start and end for all requests to a restify
 * server you can do:
 *
 *     var evt_tracer = require('evt-tracer');
 *
 *     evt_tracer.restifyTracer(server, {
 *         appClients: <Array of client handle names>,
 *         appHandle: <Object with those client handles>,
 *         logger: <Bunyan logger>
 *     });
 *
 * among your usual server.use() lines when setting up the server. This will
 * cause each request to log "evt" messages to the bunyan logger for each
 * request. It will also add a getHandle() property to "req" objects which
 * returns an API client decorated with a .traceHandle property. This
 * traceHandle property contains the information required to add the trace and
 * span headers to outbound requests and also can log these.
 *
 * Span
 * Trace
 * Logs (annotations)
 * Tags (binary annotations)
 *
 */


var assert = require('assert-plus');
var uuid = require('node-uuid');

function Span(opts) {
    var self = this;

    assert.object(opts, 'opts');
    assert.object(opts.logger, 'opts.logger');
    assert.string(opts.operation, 'opts.operation');
    if (opts.parent_span_id !== '0') {
        assert.optionalUuid(opts.parent_span_id, 'opts.parent_span_id');
    }
    assert.optionalUuid(opts.span_id, 'opts.span_id');
    assert.optionalUuid(opts.trace_id, 'opts.trace_id');

    self.creator = false;
    if (!opts.span_id) {
        // keep track of the fact that we're the creator so we can end it
        self.creator = true;
    }

    self.finished = false;
    self.logger = opts.logger;
    self.operation = opts.operation;
    self.parent_span_id = opts.parent_span_id || "0";
    self.span_id = opts.span_id || uuid.v4();
    self.tags = {};
    self.trace_id = opts.trace_id || uuid.v4();

    // Ensure this span has everything it needs
    assert.object(self.logger, 'self.logger');
    assert.string(self.operation, 'self.operation');
    if (opts.parent_span_id !== '0') {
        assert.uuid(self.parent_span_id, 'self.parent_span_id');
    }
    assert.uuid(self.span_id, 'self.span_id');
    assert.uuid(self.trace_id, 'self.trace_id');
}

Span.prototype.startChild = function startChild(opts) {
    var self = this;
    var logger;

    assert.object(opts, 'opts');
    assert.string(opts.operation, 'opts.operation');

    return (new Span({
        logger: self.logger,
        operation: opts.operation,
        // we keep the parent_span_id and trace_id, but purposefully don't
        // include a span_id since this is a new span.
        parent_span_id: self.span_id,
        trace_id: self.trace_id
    }));
};

Span.prototype.injectHeaders = function injectHeaders(headersObj) {
    var self = this;

    headersObj['x-span-id'] = self.span_id;
    headersObj['x-parent-span-id'] = self.parent_span_id;
    // We include the trace id as x-request-id for historical reasons.
    headersObj['x-request-id'] = self.trace_id;
};

Span.prototype.addTags = function addTags(tags) {
    var self = this;
    var keys;

    // ensure we're not writing to a finished span
    assert.equal(self.finished, false, 'self.finished');

    assert.object(tags, 'tags');

    keys = Object.keys(tags);

    for (var idx = 0; idx < keys.length; idx++) {
        self.tags[keys[idx]] = tags[keys[idx]];
    }
};

Span.prototype.log = function spanLog(name, end) {
    var self = this;
    var evt = {};

    // ensure we're not writing to a finished span
    assert.equal(self.finished, false, 'self.finished');

    assert.string(name, 'name');
    assert.optionalBool(end, 'end');

    // First take bits from this Span
    evt.kind = name;
    evt.operation = self.operation;
    evt.parent_span_id = self.parent_span_id;
    evt.span_id = self.span_id;
    evt.trace_id = self.trace_id;

    if (end) {
        /*
         * We only log the fact that we're ending if we actually created the
         * span. But even if we didn't, we mark the fact that we're done with
         * it so that we can prevent additional logs.
         */
        if (self.creator) {
            evt.end = true;
        }
        self.finished = true;
    }

    // consume the tags
    evt.tags = self.tags;
    self.tags = {};

    self.logger.info({evt: evt});
};


function Tracer() {
}

Tracer.newSpan = function newSpan(opts) {
    return (new Span(opts));
};

Tracer.joinSpan = function joinSpan(opts) {
    // To join, parent_span_id and trace_id are not optional, since they're
    // needed to connect us to the rest of the trace.
    assert.object(opts, 'opts');
    if (opts.parent_span_id !== '0') {
        assert.uuid(opts.parent_span_id, 'opts.parent_span_id');
    }
    assert.uuid(opts.trace_id, 'opts.trace_id');

    return (new Span(opts));
};

Tracer.clientRequest = function clientRequest(span, opts, tags) {
    assert.object(opts, 'opts');
    assert.object(opts.headers, 'opts.headers');
    assert.string(opts.operation, 'opts.operation');
    assert.object(tags, 'tags');

    var _span;

    if (span) {
        // If we have a request that we're handling, we'll already have an
        // active span. In that case, just create a new span.
        _span = span.startChild({operation: opts.operation});
    } else {
        // If we don't have an active request, we'll not have a span. So we'll
        // create one here.
        _span = Tracer.newSpan({
            logger: opts.logger,
            operation: opts.operation
        });
    }
    _span.injectHeaders(opts.headers);
    _span.addTags(tags);
    _span.log('client.request');

    return (_span);
};

Tracer.clientResponse = function clientResponse(span, opts, tags) {
    var end = true;

    assert.object(opts, 'opts');
     // opts.continue will be true if we don't want to end span here
    assert.optionalBool(opts.continue, 'opts.continue');
    assert.object(tags, 'tags');

    if (opts.continue) {
        end = false;
    }

    span.addTags(tags);
    span.log('client.response', end);
};

Tracer.serverRequest = function serverRequest(opts, tags) {
    assert.object(opts, 'opts');
    assert.object(opts.logger, 'opts.logger');
    assert.string(opts.operation, 'opts.operation');
    assert.object(opts.req, 'opts.req');

    // XXX TODO guard against bad clients

    var span = Tracer.joinSpan({
        logger: opts.logger,
        operation: opts.operation,

        /*
         * If a parent-span was passed, that's *our* parent in this trace.
         * Otherwise, we're a top-level request so we set parent = 0.
         */
        parent_span_id: opts.req.header('x-parent-span-id') || '0',

        /*
         * Every request is a different span, so if we're not passed a span-id,
         * we'll just generate our own (happens in Span()).
         */
        span_id: opts.req.header('x-span-id'),

        /*
         * Just so we use the standard terminology elsewhere, we'll add trace_id
         * as an alias for req_id which we pass through for all spans in this
         * trace.
         */
        trace_id: opts.req.getId()
    });
    span.addTags(tags);
    span.log('server.request');

    return (span);
};

Tracer.serverResponse = function serverResponse(span, opts, tags) {
    var end = true;

    assert.object(opts, 'opts');
     // opts.continue will be true if we don't want to end span here
    assert.optionalBool(opts.continue, 'opts.continue');
    assert.object(tags, 'tags');

    if (opts.continue) {
        end = false;
    }

    span.addTags(tags);
    span.log('server.response', end);
};


function traceRestifyRead(traceHandle, origRead) {
    return function _traceWrappedRead(opts, cb) {
        var self = this;
	var span;

        // TODO: research whether these are guaranteed
        assert.object(opts, 'opts');
        assert.string(opts.method, 'opts.method');
        assert.string(opts.path, 'opts.path');
        assert.string(opts.href, 'opts.href');

        span = Tracer.clientRequest(traceHandle, {
            headers: opts.headers,
            operation: 'restifyclient.' + opts.method
        }, {
	    'http.method': opts.method.substr(0, 80),
            'http.path': opts.path.substr(0, 80),
            'http.url': opts.href.substr(0, 80)
        });

        return origRead.apply(self, [opts,
            function _traceWrappedCb(err, _, res) {
                var code = (res && res.statusCode && res.statusCode.toString());

                Tracer.clientResponse(span, {}, {
                    error: (err ? true : undefined),
                    'http.statusCode': code
                });

                // call the original cb
                return cb.apply(self, arguments);
            }
        ]);
    };
}


// For injecting ourselves into req.getHandle()

function handleMaker(req, opts) {
    var self = this;

    return function _handleMaker(name) {
        var constructor;
        var handle;

        assert(opts.appClients.indexOf(name) !== -1, 'opts.appClients.' + name);
        assert(opts.appHandle.hasOwnProperty(name), 'opts.appHandle.' + name);

        handle = Object.create(opts.appHandle[name]);

        if (typeof(opts.appHandle[name].client) === 'object'
            && opts.appHandle[name].client !== 'undefined') {

            // XXX check a version too?

            constructor = opts.appHandle[name].client.constructor.name;
            if (['JsonClient', 'RestifyClient'].indexOf(constructor) !== -1) {
                req.log.debug('hacking restify client for ' + name + ' which is'
                    + ' a ' + constructor);
                handle.client = Object.create(opts.appHandle[name].client);
                handle.client.read = traceRestifyRead(req.traceHandle,
                    opts.appHandle[name].client.read);
            } else {
                req.log.debug('not hacking restify client for ' + name
                    + ' which is a ' + constructor);
                handle.traceHandle = req.traceHandle;
            }
        } else {
            req.log.debug(name + ' does not have restify .client, but rather: '
                + typeof(opts.appHandle[name].client));
            handle.traceHandle = req.traceHandle;
        }

        return (handle);
    };
};

/*
 * should go near the beginning of your server.use() bits. Definitely before
 * anything that might make a client connection.
 */
Tracer.restifyTracer = function restifyTracer(server, opts) {
    var self = this;

    assert.object(opts.logger, 'opts.logger');
    assert.string(opts.logger.fields.name, 'opts.logger.fields.name');

    /*
     * We do server.use instead of server.on('request', ...) because the request
     * event is emitted before we've got the route.name.
     */
    server.use(function (req, res, next) {
        /*
         * Each req object "joins" the span of the incomming request that we're
         * handling. As clients create new requests, those become children with
         * startChild().
         *
         * We use the opts.logger.fields.name as the name here because that's
         * the name that'll be in the logs already.
         */
        var span = Tracer.serverRequest({
            logger: opts.logger,
            operation: opts.logger.fields.name + '.'
                + (req.route ? req.route.name : 'UNKNOWN'),
            req: req
        }, {
            'peer.addr': req.connection.remoteAddress,
            'peer.port': req.connection.remotePort
        });

        req.traceHandle = span;

        /*
         * Build a closure accessor for grabbing client handles so that we can
         * include trace context.
         */
        req.getHandle = handleMaker(req, opts);

        res.on('header', function _addSpanHeader() {
            // send our span_id header in all responses XXX: do we actually need this?
            if (req.traceHandle && req.traceHandle.span_id) {
                res.header('x-span-id', req.traceHandle.span_id);
            }
        });

        next();
    });

    server.on('after', function (req, res, route, err) {
        if (req.traceHandle) {
            Tracer.serverResponse(req.traceHandle, {}, {
                'http.statusCode': res.statusCode.toString()
            });
        }
    });
};

Tracer.decorateReq = function decorateReq(req, opts) {
    var self = this;

    assert.object(opts.logger, 'opts.logger');
    assert.string(opts.logger.fields.name, 'opts.logger.fields.name');

    /*
     * Build a closure accessor for grabbing client handles so that we can
     * include trace context.
     */
    req.getHandle = handleMaker(req, opts);
};

module.exports = Tracer;
