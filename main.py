from datetime import datetime, timedelta

try:
    import botostubs
except:
    pass
import boto3


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def get_eligible_instance_types(ec2, min_vcpu_count, min_memory_mib):
    type_list = [
        't1.micro',
        't2.nano',
        't2.micro',
        't2.small',
        't2.medium',
        't2.large',
        't2.xlarge',
        't2.2xlarge',
        't3.nano',
        't3.micro',
        't3.small',
        't3.medium',
        't3.large',
        't3.xlarge',
        't3.2xlarge',
        't3a.nano',
        't3a.micro',
        't3a.small',
        't3a.medium',
        't3a.large',
        't3a.xlarge',
        't3a.2xlarge',
        'm1.small',
        'm1.medium',
        'm1.large',
        'm1.xlarge',
        'm3.medium',
        'm3.large',
        'm3.xlarge',
        'm3.2xlarge',
        'm4.large',
        'm4.xlarge',
        'm4.2xlarge',
        'm4.4xlarge',
        'm4.10xlarge',
        'm4.16xlarge',
        'm2.xlarge',
        'm2.2xlarge',
        'm2.4xlarge',
        'cr1.8xlarge',
        'r3.large',
        'r3.xlarge',
        'r3.2xlarge',
        'r3.4xlarge',
        'r3.8xlarge',
        'r4.large',
        'r4.xlarge',
        'r4.2xlarge',
        'r4.4xlarge',
        'r4.8xlarge',
        'r4.16xlarge',
        'r5.large',
        'r5.xlarge',
        'r5.2xlarge',
        'r5.4xlarge',
        'r5.8xlarge',
        'r5.12xlarge',
        'r5.16xlarge',
        'r5.24xlarge',
        'r5.metal',
        'r5a.large',
        'r5a.xlarge',
        'r5a.2xlarge',
        'r5a.4xlarge',
        'r5a.8xlarge',
        'r5a.12xlarge',
        'r5a.16xlarge',
        'r5a.24xlarge',
        'r5d.large',
        'r5d.xlarge',
        'r5d.2xlarge',
        'r5d.4xlarge',
        'r5d.8xlarge',
        'r5d.12xlarge',
        'r5d.16xlarge',
        'r5d.24xlarge',
        'r5d.metal',
        'r5ad.large',
        'r5ad.xlarge',
        'r5ad.2xlarge',
        'r5ad.4xlarge',
        'r5ad.8xlarge',
        'r5ad.12xlarge',
        'r5ad.16xlarge',
        'r5ad.24xlarge',
        'x1.16xlarge',
        'x1.32xlarge',
        'x1e.xlarge',
        'x1e.2xlarge',
        'x1e.4xlarge',
        'x1e.8xlarge',
        'x1e.16xlarge',
        'x1e.32xlarge',
        'i2.xlarge',
        'i2.2xlarge',
        'i2.4xlarge',
        'i2.8xlarge',
        'i3.large',
        'i3.xlarge',
        'i3.2xlarge',
        'i3.4xlarge',
        'i3.8xlarge',
        'i3.16xlarge',
        'i3.metal',
        'i3en.large',
        'i3en.xlarge',
        'i3en.2xlarge',
        'i3en.3xlarge',
        'i3en.6xlarge',
        'i3en.12xlarge',
        'i3en.24xlarge',
        'i3en.metal',
        'hi1.4xlarge',
        'hs1.8xlarge',
        'c1.medium',
        'c1.xlarge',
        'c3.large',
        'c3.xlarge',
        'c3.2xlarge',
        'c3.4xlarge',
        'c3.8xlarge',
        'c4.large',
        'c4.xlarge',
        'c4.2xlarge',
        'c4.4xlarge',
        'c4.8xlarge',
        'c5.large',
        'c5.xlarge',
        'c5.2xlarge',
        'c5.4xlarge',
        'c5.9xlarge',
        'c5.12xlarge',
        'c5.18xlarge',
        'c5.24xlarge',
        'c5.metal',
        'c5d.large',
        'c5d.xlarge',
        'c5d.2xlarge',
        'c5d.4xlarge',
        'c5d.9xlarge',
        'c5d.12xlarge',
        'c5d.18xlarge',
        'c5d.24xlarge',
        'c5d.metal',
        'c5n.large',
        'c5n.xlarge',
        'c5n.2xlarge',
        'c5n.4xlarge',
        'c5n.9xlarge',
        'c5n.18xlarge',
        'cc1.4xlarge',
        'cc2.8xlarge',
        'g2.2xlarge',
        'g2.8xlarge',
        'g3.4xlarge',
        'g3.8xlarge',
        'g3.16xlarge',
        'g3s.xlarge',
        'g4dn.xlarge',
        'g4dn.2xlarge',
        'g4dn.4xlarge',
        'g4dn.8xlarge',
        'g4dn.12xlarge',
        'g4dn.16xlarge',
        'cg1.4xlarge',
        'p2.xlarge',
        'p2.8xlarge',
        'p2.16xlarge',
        'p3.2xlarge',
        'p3.8xlarge',
        'p3.16xlarge',
        'p3dn.24xlarge',
        'd2.xlarge',
        'd2.2xlarge',
        'd2.4xlarge',
        'd2.8xlarge',
        'f1.2xlarge',
        'f1.4xlarge',
        'f1.16xlarge',
        'm5.large',
        'm5.xlarge',
        'm5.2xlarge',
        'm5.4xlarge',
        'm5.8xlarge',
        'm5.12xlarge',
        'm5.16xlarge',
        'm5.24xlarge',
        'm5.metal',
        'm5a.large',
        'm5a.xlarge',
        'm5a.2xlarge',
        'm5a.4xlarge',
        'm5a.8xlarge',
        'm5a.12xlarge',
        'm5a.16xlarge',
        'm5a.24xlarge',
        'm5d.large',
        'm5d.xlarge',
        'm5d.2xlarge',
        'm5d.4xlarge',
        'm5d.8xlarge',
        'm5d.12xlarge',
        'm5d.16xlarge',
        'm5d.24xlarge',
        'm5d.metal',
        'm5ad.large',
        'm5ad.xlarge',
        'm5ad.2xlarge',
        'm5ad.4xlarge',
        'm5ad.8xlarge',
        'm5ad.12xlarge',
        'm5ad.16xlarge',
        'm5ad.24xlarge',
        'h1.2xlarge',
        'h1.4xlarge',
        'h1.8xlarge',
        'h1.16xlarge',
        'z1d.large',
        'z1d.xlarge',
        'z1d.2xlarge',
        'z1d.3xlarge',
        'z1d.6xlarge',
        'z1d.12xlarge',
        'z1d.metal',
        'u-6tb1.metal',
        'u-9tb1.metal',
        'u-12tb1.metal',
        'u-18tb1.metal',
        'u-24tb1.metal',
        'a1.medium',
        'a1.large',
        'a1.xlarge',
        'a1.2xlarge',
        'a1.4xlarge',
        'a1.metal',
        'm5dn.large',
        'm5dn.xlarge',
        'm5dn.2xlarge',
        'm5dn.4xlarge',
        'm5dn.8xlarge',
        'm5dn.12xlarge',
        'm5dn.16xlarge',
        'm5dn.24xlarge',
        'm5n.large',
        'm5n.xlarge',
        'm5n.2xlarge',
        'm5n.4xlarge',
        'm5n.8xlarge',
        'm5n.12xlarge',
        'm5n.16xlarge',
        'm5n.24xlarge',
        'r5dn.large',
        'r5dn.xlarge',
        'r5dn.2xlarge',
        'r5dn.4xlarge',
        'r5dn.8xlarge',
        'r5dn.12xlarge',
        'r5dn.16xlarge',
        'r5dn.24xlarge',
        'r5n.large',
        'r5n.xlarge',
        'r5n.2xlarge',
        'r5n.4xlarge',
        'r5n.8xlarge',
        'r5n.12xlarge',
        'r5n.16xlarge',
        'r5n.24xlarge',
        'inf1.xlarge',
        'inf1.2xlarge',
        'inf1.6xlarge',
        'inf1.24xlarge',
    ]
    page_results = []
    for group in chunks(type_list, 100):
        params = {'InstanceTypes': group}
        while True:
            try:
                result = ec2.describe_instance_types(**params)
            except Exception as e:
                error = e.response['Error']
                msg = error['Message']
                if error['Code'] == 'InvalidInstanceType' and 'types do not exist:' in msg:
                    invalid_ones = msg[msg.index('[') + 1 : -1].split(', ')
                    params['InstanceTypes'] = list(set(group).difference(set(invalid_ones))) or []
                    continue
            page_results.extend(result['InstanceTypes'])  # yield from
            if 'NextToken' not in result:
                break
            print('paginating')
            params['NextToken'] = result['NextToken']
    final_results = []
    for item in page_results:
        if 'spot' not in item['SupportedUsageClasses']:
            continue
        memory = item['MemoryInfo']['SizeInMiB']
        if min_memory_mib > memory:
            continue
        cpu = item['VCpuInfo']
        if min_vcpu_count > cpu['DefaultVCpus']:
            continue
        if 'ValidCores' in cpu:
            max_valid_core = sorted(cpu['ValidCores'])[-1]
            if cpu['DefaultCores'] != max_valid_core:
                print(f'default core count is not the maximum for instance type {item["InstanceType"]}')
        if 'ValidThreadsPerCore' in cpu:
            max_valid_threads_per_core = sorted(cpu['ValidThreadsPerCore'])[-1]
            if cpu['DefaultThreadsPerCore'] != max_valid_threads_per_core:
                print(f'default thread core count is not the maximum for instance type {item["InstanceType"]}')
        final_results.append(item)
    print(f'Got {len(final_results)} eligible ones')
    return final_results


def go_region(region, result_instance_types, max_price_usd_per_hour, min_vcpu_count, min_memory_mib):
    ec2 = boto3.client('ec2', region_name=region)  # type: botostubs.EC2
    print('Getting instance types eligible for spot in region', region)
    instance_types = get_eligible_instance_types(ec2, min_vcpu_count, min_memory_mib)
    azs = ec2.describe_availability_zones()['AvailabilityZones']
    for az_info in azs:
        az = az_info['ZoneName']
        print(f'now in {az}')
        now = datetime.utcnow()
        start_time = now - timedelta(hours=1)
        params = {
            'AvailabilityZone': az,
            'InstanceTypes': list(map(lambda x: x['InstanceType'], instance_types)),
            'StartTime': start_time,
        }
        while True:
            result = ec2.describe_spot_price_history(**params)
            for item in result['SpotPriceHistory']:
                price = float(item['SpotPrice'])
                if price <= max_price_usd_per_hour:  # and 'linux' in item['ProductDescription'].lower():
                    it = item['InstanceType']
                    ts = item['Timestamp']
                    print(f'{it} was valued at ${price}/hour at around {ts} in {az}')
                    result_instance_types.append(item)
            if not result['NextToken']:
                break
            params['NextToken'] = result['NextToken']
            params['StartTime'] = params['StartTime'] + timedelta(hours=1)
    return result_instance_types


def go(max_price_usd_per_hour, min_vcpu_count, min_memory_mib):
    max_price_usd_per_hour = float(max_price_usd_per_hour)
    min_vcpu_count = float(min_vcpu_count)
    min_memory_mib = float(min_memory_mib)
    print('Got max price $', max_price_usd_per_hour, '/hour')
    print('Got min vcpu count:', min_vcpu_count)
    print('Got minimum memory (MiB):', min_memory_mib)
    client = boto3.client('ec2')  # type: botostubs.EC2
    regions = list(map(lambda x: x['RegionName'], client.describe_regions()['Regions']))
    print('got regions', regions)
    result_instance_types = []
    for region in regions:
        go_region(region, result_instance_types, max_price_usd_per_hour, min_vcpu_count, min_memory_mib)
    sorted_result = list(
        sorted(
            result_instance_types, key=lambda x: x['SpotPrice'] + '-' + x['AvailabilityZone'] + '-' + x['InstanceType']
        )
    )
    print('\n\nOrder in which to consider requesting spot instances:')
    for item in sorted_result:
        item['Timestamp'] = str(item['Timestamp'])
        print(item)


if __name__ == '__main__':
    import sys

    go(sys.argv[1], sys.argv[2], sys.argv[3])
