# How to publish artifacts to the Apache repository

## Prerequisites

Only PMC members have access for pushing artifacts to the Apache repository.

Setup your development env according to [this instruction](http://www.apache.org/dev/publishing-maven-artifacts.html#dev-env).

Set passphrase variable without putting it into shell history:

`read -s GPG_PASSPHRASE`

## Deploy artifacts with signatures to the staging repository

Change directory to `drill/drill-shaded/drill-shaded-guava`.

* For the case when `maven-gpg-plugin` plugin wasn’t added to the `pom.xml`, run

`mvn clean verify gpg:sign install:install deploy:deploy -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -Dgpg.keyname=${GPG_KEY_ID}"`

* For the case of configured `maven-gpg-plugin` plugin, run

`mvn deploy -Darguments="-Dgpg.passphrase=${GPG_PASSPHRASE} -Dgpg.keyname=${GPG_KEY_ID}"`

## Visit [repository.apache.org](https://repository.apache.org/#stagingRepositories)

* Log in using your Apache credentials
* Select uploaded staging repository
* Check that all required `jar` and `pom` files are uploaded
* Verify that every `jar` and `pom` file has a corresponding signature file (*.asc)

If something went wrong, press `Drop` to remove the staging repository.

## Close staging repository

Select uploaded staging repository and press `Close` button.

Verify that all checks are passed and staging repository was closed. Otherwise, drop staging repository and fix errors.

## Publish artifacts to the Apache repository

Select uploaded staging repository and press `Release` button.

## Check artifacts were deployed

Find deployed artifacts at [repository.apache.org](https://repository.apache.org/content/groups/public/org/apache/drill/)

Artifacts will become available within 24 hours.
