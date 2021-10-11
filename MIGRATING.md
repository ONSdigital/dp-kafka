# Migration to AWS MSK

```text
Please remember to come back to this documentation after completing each individual step
```

1. [Add kafka topics to manifest](README.md#1-add-kafka-topics-to-manifest)

2. [Create client certificate for the app - Run `key-admin` script](README.md#2-create-client-certificate-for-the-app---run-key-admin-script)
   - **Do not merge the amended secrets after running the script. This should be done at STEP 7**
  
3. [Apply kafka topics to AWS MKS - Run `topic-manager` script](README.md#3-apply-kafka-topics-to-aws-mks---run-topic-manager-script)

4. [Add configs in the app to use AWS MSK](README.md#4-add-configs-in-the-app-to-use-aws-msk)

5. [Update creating kafka producer and/or consumer with initialising `SecurityConfig`](README.md#creation)

6. Deploy app
   - The app will still be using old kafka but it is ready to use AWS MSK. This is because TLS is not enabled by default
  
7. Deploy amended `secrets` from STEP 2 with configs to enable TLS connection

8. Redeploy app to pick up the amended secrets and to use AWS MSK
