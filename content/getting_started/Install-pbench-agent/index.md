---
title: 'Install Pbench Agent'
date: 2019-02-11T19:27:37+10:00
weight: 1
summary: Installing Pbench Agent
---

#### Table of Contents
---
- Introduction
- Release announcements   internal
- Boostrapping a New System   internal
- Installing Using Ansible   internal
    - Install ansible and ansible-galaxy (one-time task)
    - Install the pbench.agent Roles from Galaxy (one-time task)        
        - Backward Compatibility
    - Make Roles Available (Galaxy only).
    - Create a Playbook (one-time task)
    - Create an Inventory File (one-time task for a given set of hosts)
    - Run the playbook
    - (Optional) Some useful ad-hoc commands
    - Repo files for different environments   internal

---
#### Introduction

The pbench agent requires the installation of some generic bits, but it also requires some localization: it needs to know where to send the results for storage and analysis and it needs to be able to authenticate to the results server.

The generic bits are packaged as an RPM, available from COPR (https://copr.fedorainfracloud.org/). We try to build a new release every month or so.

The localization bits are of course, specific to a particular installation. Internally, we make them available through an ansible playbook. We used to make them available through an internal RPM, but we have deprecated that method: we no longer build internal RPMs. Other installations may use different methods to make them available. Consult your local pbench guru for help.

In the following, we describe how to install `pbench-agent` using an ansible playbook.

Note that the same `pbench-agent` version **must** be installed on all the test systems that participate in a benchmark run: there is **no support for mixed installations**.

