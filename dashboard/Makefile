VER		= 0.0.1
NAME 	= pbench-dashboard
USER_NAME = ${USER}

build-container-image:
	buildah build -f ./container/Dockerfile -t $(USER_NAME)/$(NAME)-v$(VER) .

build-release-tarball:
	mkdir $(NAME)-$(VER)
	rsync -a --exclude=container --exclude=node_modules --exclude=build --exclude=$(NAME)-$(VER) --exclude=*.tar.gz . $(NAME)-$(VER)
	tar -czcf $(NAME)-$(VER).tar.gz $(NAME)-$(VER)/
	rm -rf $(NAME)-$(VER)
