package _Self

import jetbrains.buildServer.configs.kotlin.*
import jetbrains.buildServer.configs.kotlin.Project
import jetbrains.buildServer.configs.kotlin.vcs.GitVcsRoot


object Project : Project({

    description = "Build / Publish pipeline for https://github.com/carbonengine/grpc"

    params {
        /* before changing carbon_ref, make sure to disable automatic settings synchronization on teamcity */
        param("carbon_ref", "refs/heads/main")
        param("carbon-pipeline-tools-ref", "refs/heads/main")
    }
    
    subProject(Windows.Project)
    subProject(MacOS.Project)
})
