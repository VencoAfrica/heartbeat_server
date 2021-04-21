
class HeartbeartData:
    def __init__(self, data):
        self._data = data
    
    @property
    def version_number(self):
        return self._data[0:2]

    @property
    def source_address(self):
		return self._data[2:4]

    @property
    def target_address(self):
		return self._data[4:6]

	@property
    def frame_length(self):
        return self._data[6:8]

    @property
    def fixed_format(self):
        return self._data[8:13]

    @property
    def structure(self):
        return self._data[13:15]

    @property
    def visible_string(self):
        return self._data[15:16]

    @property
    def device_details_length(self):
        return self._data[16:17]

    @property
    def device_details_length_int(self):
        return int.from_bytes(self.device_details_length, byteorder='big')

    @property
    def device_details(self):
        return self._data[17: 17 + self.device_details_length_int]

    @property
    def double_long_unsigned(self):
        return self._data[17 + self.device_details_length_int:
                          17 + self.device_details_length_int + 1]

    @property
    def address(self):
        return self._data[17 + self.device_details_length_int + 1:
                          17 + self.device_details_length_int + 5]

    @property
    def output_data(self):
		''' create out_data remember to transports the target_address and source_address '''
		return  self.version_number + 
                self.target_address + 
                self.source_address + 
                len(self.fixed_format).to_bytes(2, 'big') + 
                self.fixed_format
