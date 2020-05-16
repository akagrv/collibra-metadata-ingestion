def getCommunityObj(communityName, parentCommunityName = None):
    comm_obj = {}
    comm_obj['resourceType'] = 'Community'
    comm_obj['identifier'] = {
        'name': communityName
    }
    if parentCommunityName != None:
        comm_obj['parent'] = {
            'name': parentCommunityName
        }
    return comm_obj

def getDomainObj(communityName, domainName, domainType):
    obj = {}
    obj['resourceType'] = 'Domain'
    obj['identifier'] = {
        'name': domainName,
        'community': {
            'name': communityName
        }
    }
    obj['type'] = {
        'name': domainType
    }
    return obj

def getAssetObj(communityName, domainName, assetName, assetType, displayName = None, attributes = None, relations = None):
    obj = {}
    obj['resourceType'] = 'Asset'
    obj['identifier'] = {
        'name': assetName,
        'domain': {
            'name': domainName,
            'community': {
                'name': communityName
            }
        }
    }
       
    if attributes != None:
        attributes_obj = {}
        
        if not type(attributes[0]) is tuple:
            attributes_obj[attributes[0]] = [{
                'value': attributes[1]
            }]
        else:
            for attributeKey, attributeValue in attributes:
                attributes_obj[attributeKey] = [{
                    'value': attributeValue
                }]
        
        obj['attributes'] = attributes_obj
    
    if relations != None:
        obj['relations'] = {
            relations[0]: [
                {
                    'name': relations[1],
                    'domain': {
                        'name': domainName,
                        'community': {
                            'name': communityName
                        }
                    }        
                }
            ]
        }
    
    if displayName != None:
        obj['displayName'] = displayName
    obj['type'] = {
        'name': assetType
    }
    return obj