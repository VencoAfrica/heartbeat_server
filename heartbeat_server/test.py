obis_codes = {
    'voltage': '32.7.0.255',
    'time': '0.9.1.255',
}

ccu_meters = {
    'MTRK179000229718': ['MTRK179000229718'],
    'MTRK179000989725': [
        'MTRK179000989725',
        '179000983249',
        '179000980526',
        '179000983397',
        '179000983009',
        '179000983348',
        '179000982720',
        '179000983355',
        '179000980518',
        '179000983330'
    ]}


def get_reading_cmds(ccu_no):
    read_commands = [
        (
            meter,
            cmd,
            "CommandMessage"
        )
        for cmd, obis_code in obis_codes.items()
        for meter in ccu_meters.get(ccu_no)
    ]
    return read_commands


print(get_reading_cmds('MTRK179000989725'))