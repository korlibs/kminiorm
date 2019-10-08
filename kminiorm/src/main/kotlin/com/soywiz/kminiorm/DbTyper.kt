package com.soywiz.kminiorm

import com.soywiz.kminiorm.typer.*
import java.util.*

val DbTyper = Typer()
        .withKeepType<Date>()
        .withKeepType<UUID>()
