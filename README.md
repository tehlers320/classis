# Classis

Query aws and report all instanace types to graphite


## config example

```
{
  "region": "ap-southeast-2",
  "stream": "stream-name",
  "role": "arn:aws:iam::839025751174:role/my_aws_describe_instances_role",
  "inspection_roles": [
    {
      "role": "arn:aws:iam::839025751174:role/aws_describe_permissions"
    }
  ]
}
```