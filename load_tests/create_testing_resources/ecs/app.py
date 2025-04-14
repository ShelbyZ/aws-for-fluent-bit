import os
from constructs import Construct
from aws_cdk import App, Stack, RemovalPolicy, CfnOutput, Fn
from aws_cdk import (
    aws_logs as logs,
    aws_autoscaling as autoscaling,
    aws_ec2 as ec2,
    aws_ecs as ecs,
    aws_iam as iam
)

# Create necessary ECS load testing resources - cloudwatch log group and ecs cluster 
class TestingResources(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        log_group = logs.LogGroup(self, 'logGroup',
                                  removal_policy=RemovalPolicy.DESTROY)
 
        # Resources for ecs ec2 testing
        vpc = ec2.Vpc(
            self, "vpc",
            max_azs=2
        )
        vpc.apply_removal_policy(RemovalPolicy.DESTROY)
        cluster = ecs.Cluster(
            self, 'ecsCluster',
            vpc=vpc
        )
        # Create the user data for ECS
        user_data = ec2.UserData.for_linux()

        role = iam.Role(self, "FleetInstanceRole", assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"))
        # role.add_to_policy(iam.PolicyStatement(
        #     actions=["ecs:DeregisterContainerInstance", "ecs:RegisterContainerInstance", "ecs:Submit*"
        #     ],
        #     resources=[cluster.cluster_arn]
        # ))
        # role.add_to_policy(iam.PolicyStatement(
        #     actions=["ecs:Poll", "ecs:StartTelemetrySession", "ecs:DiscoverPollEndpoint",
        #         "ecr:GetAuthorizationToken",
        #         "logs:CreateLogStream",
        #         "logs:PutLogEvents"
        #     ],
        #     resources=['*']
        # ))

        launch_template = ec2.LaunchTemplate(
            self, "ECSLaunchTemplate-FB",
            instance_type=ec2.InstanceType("c7a.24xlarge"),
            machine_image=ecs.EcsOptimizedImage.amazon_linux2(),
            associate_public_ip_address=True,
            user_data=user_data,
            require_imdsv2=True,
            role=role
        )

        asg = autoscaling.AutoScalingGroup(
            self, "fleet",
            launch_template=launch_template,
            desired_capacity=5,
            vpc=vpc,
            vpc_subnets={ 'subnet_type': ec2.SubnetType.PUBLIC },
        )
        asg.apply_removal_policy(RemovalPolicy.DESTROY)
        capacity_provider = ecs.AsgCapacityProvider(self, "asgCapacityProvider",
            auto_scaling_group=asg,
            enable_managed_termination_protection=True
        )
        cluster.add_asg_capacity_provider(capacity_provider)
        cluster.apply_removal_policy(RemovalPolicy.DESTROY)

        # Add stack outputs
        CfnOutput(self, 'CloudWatchLogGroupName', 
                  value=log_group.log_group_name, 
                  description='CloudWatch Log Group Name')
        
        CfnOutput(self, "ECSClusterName", 
                  value=cluster.cluster_name, 
                  description="ECS Cluster Name")

app = App()
TestingResources(app, os.environ['TESTING_RESOURCES_STACK_NAME'])
app.synth()
