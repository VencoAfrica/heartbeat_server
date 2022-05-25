class CommandTemplate:
    def __init__(self, code):
        self.code = code
        self.error_idx = None
        self.indexes = []
        self.parts = []

        parts = self.get_parts(code)
        if self.error_idx is None and parts:
            self.parts, self.indexes = parts

    def get_parts(self, code=None):
        OPEN, CLOSE = '{', '}'
        self.error_idx = None
        code = self.code if code is None else code
        parts = []
        indexes = []

        idx = 0
        open_idx = None
        for i, ch in enumerate(code):
            if ch == OPEN:
                if open_idx is not None:
                    self.error_idx = i
                    break
                open_idx = i
                parts.append(code[idx:i])
                idx = i
            elif ch == CLOSE:
                if open_idx is not None and i > open_idx + 1:
                    parts.append(code[idx:i + 1])
                    indexes.append(len(parts) - 1)
                    idx = i + 1
                    open_idx = None
                else:
                    self.error_idx = i
                    break

        if open_idx is not None and self.error_idx is None:
            self.error_idx = open_idx
        if self.error_idx is None:
            return parts, indexes
        return []

    def validate(self):
        idx = self.error_idx
        if idx is not None:
            frappe.throw(
                'Error in obis code, unexpected %r at index %s' % (self.code[idx], idx))

        seen = {}
        variables = self.get_variables()
        for variable in variables:
            if variable in seen:
                frappe.throw('Duplicate variable in obis code: %s' % variable)
            seen[variable] = True
        return True

    def get_variables(self, ):
        variables = []
        for idx in self.indexes:
            part = self.parts[idx]
            variables.append(part[1:-1])
        return variables

    def parse(self, **values_dict):
        variables = {v.lower(): i for i, v in enumerate(self.get_variables())}
        parts = self.parts[:]
        for key, val in values_dict.items():
            key = key.lower()
            if key in variables:
                parts[self.indexes[variables[key]]] = val
        return ''.join(str(i) for i in parts)