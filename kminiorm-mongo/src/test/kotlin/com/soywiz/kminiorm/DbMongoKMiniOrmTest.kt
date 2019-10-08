package com.soywiz.kminiorm

import io.vertx.core.*

class DbMongoKMiniOrmTest : KMiniOrmBaseTests(Vertx.vertx().createMongo("mongodb://127.0.0.1:27017/kminiormtest"))
