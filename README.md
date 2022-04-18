[pbench-website](http://distributed-system-analysis.github.io/pbench/)

- Contact page  :  ```content/getting_started/Contact/index.md```
- Demo page     :  ```content/getting_started/Demo/index.md```
- Docs page     :  ```content/getting_started/Docs/index.md```
- Install pbench agent:     ```content/getting_started/Install-pbench-agent/index.md```
- Images & media resources: ```static/images/```

### Project Setup

Make sure hugo is installed in your machine, if not then install it using
```
$ dnf install hugo  # fedora system
```
or 

try to look for binary [here](https://github.com/gohugoio/hugo/releases)


```
# git clone with submodules flag
$ git clone --recurse-submodules git@github.com:distributed-system-analysis/pbench.git
or
$ git clone --recurse-submodules https://github.com/distributed-system-analysis/pbench.git

$ hugo server    //run local instance 
```

[Hugo Theme](https://github.com/zerostaticthemes/hugo-whisper-theme)