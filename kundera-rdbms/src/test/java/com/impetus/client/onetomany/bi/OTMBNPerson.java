package com.impetus.client.onetomany.bi;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;


@Entity
@Table(name="PERSON", schema="test")
public class OTMBNPerson
{
    @Id   
    @Column(name="PERSON_ID")    
    private String personId;
    
    @Column(name="PERSON_NAME")
    private String personName;
    
    @ManyToOne
    @JoinColumn(name = "ADDRESS_ID")
    private OTMBAddress address;

    public OTMBNPerson()
    {
        
    }
    
    public String getPersonId()
    {
        return personId;
    }   
    

    public String getPersonName()
    {
        return personName;
    }

    public void setPersonName(String personName)
    {
        this.personName = personName;
    }



    public void setPersonId(String personId)
    {
        this.personId = personId;
    }


    public OTMBAddress getAddress()
    {
        return address;
    }

    /**
     * @param address the address to set
     */
    public void setAddress(OTMBAddress address)
    {
        this.address = address;
    }

}
