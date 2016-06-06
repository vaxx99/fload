package ama

import (
	"strconv"
	"time"
)

type Record struct {
	Id, Sw, Hi, Na, Nb, Ds, De, Dr, Ot, It, Du string
}

type Redrec struct {
    Id string `json:"id"`
    Sw string `json:"sw"`
    Hi string `json:"hi"`
    Na string `json:"na"`
    Nb string `json:"nb"`
    Ds string `json:"ds"`
    De string `json:"de"`
    Dr string `json:"dr"`
    Ot string `json:"ot"`
    It string `json:"it"`
    Du string `json:"du"`
}

func A9020(bcd, yy string) string {
	aa := bcd[0:2]
	aa += bcd[2:6]
	aa += bcd[6:12]
	aa += bcd[12:18]
	i, _ := strconv.Atoi(bcd[18:20])
	aa += bcd[36-i : 36]
	i, _ = strconv.Atoi(bcd[36:38])
	aa += bcd[70-i : 70]
	aa += bcd[70:74]
	aa += bcd[74:78]
	aa += bcd[78:80]
	aa += bcd[80:82]
	aa += bcd[82:84]
	aa += bcd[84:86]
	aa += bcd[86:88]
	aa += yy + bcd[89:99]
	aa += bcd[99:100]
	aa += yy + bcd[101:111]
	aa += bcd[111:112]
	aa += bcd[112:114]
	aa += bcd[114:118]
	aa += bcd[118:122]
	aa += bcd[122:126]
	aa += bcd[126:132]
	aa += bcd[132:138]
	aa += bcd[138:140]
	aa += bcd[140:148]
	aa += bcd[148:150]
	aa += bcd[150:152]
	aa += bcd[152:154]
	aa += bcd[154:156]
	aa += bcd[156:158]
	aa += bcd[158:160]
	return aa
}

func A9021(bcd, yy string) string {
	aa := bcd[0:2]                   //Hexadecimal Identifier
	aa += bcd[2:6]                   //Structure Identifier Code
	aa += bcd[6:12]                  //Ticket Number
	aa += bcd[12:18]                 //Sequence Number
	i, _ := strconv.Atoi(bcd[18:20]) //
	aa += bcd[36-i : 36]             //Originating Phone Number
	i, _ = strconv.Atoi(bcd[36:38])  //
	aa += bcd[70-i : 70]             //Terminating Phone Number
	aa += bcd[70:72]                 //Charge Category
	aa += bcd[72:74]                 //Nature of Call
	aa += bcd[74:76]                 //CDA Indicator
	aa += bcd[76:78]                 //LDC Indicator
	aa += bcd[78:80]                 //Service Class of Call
	aa += yy + bcd[81:91]            //Date and Time of Charging Commencement
	aa += bcd[91:92]                 //*ms
	aa += bcd[92:104]                //Date and Time of Call End
	aa += bcd[104:106]               //Cause of Call End
	aa += bcd[106:110]               //Destination
	aa += bcd[110:114]               //Outgoing Trunk Group
	aa += bcd[114:118]               //Incoming Trunk Group
	aa += bcd[118:124]               //Conversation Time
	aa += bcd[124:130]               //Chargeable Duration
	aa += bcd[130:132]               //Class of Rate
	aa += bcd[132:140]               //Fee
	aa += bcd[140:142]               //Trouble Mark
	aa += bcd[142:144]               //Day
	aa += bcd[144:146]               //A-Party Category
	aa += bcd[146:148]               // Type of Call
	aa += bcd[148:150]               //Customer Feature
	aa += bcd[150:152]               //Customer Feature Action
	return aa
}

func A9025(bcd, yy string) string {
	aa := bcd[0:2]   //Hexadecimal Identifier
	aa += bcd[2:6]   //Structure Identifier Code
	aa += bcd[6:12]  //Ticket Number
	aa += bcd[12:18] //Sequence Number
	i, _ := strconv.Atoi(bcd[18:20])
	aa += bcd[36-i : 36] //Originating Phone Number
	i, _ = strconv.Atoi(bcd[36:38])
	aa += bcd[70-i : 70]  //Terminating Phone Number
	aa += bcd[70:72]      //Charge Category
	aa += bcd[72:74]      //Nature of Call
	aa += bcd[74:76]      //CDA Indicator
	aa += bcd[76:78]      //LDC Indicator
	aa += bcd[78:80]      //Service Class of Call
	aa += yy + bcd[81:91] //Date and Time of Charging Commencement
	aa += bcd[91:92]
	aa += yy + bcd[93:103] //Date and Time of Call End
	aa += bcd[103:104]
	aa += bcd[104:106] //Cause of Call End
	aa += bcd[106:110] //Destination
	aa += bcd[110:114] //Outgoing Trunk Group
	aa += bcd[114:118] //Incoming Trunk Group
	aa += bcd[118:124] //Conversation Time
	aa += bcd[124:130] //Chargeable Duration
	aa += bcd[130:132] //Class of Rate
	aa += bcd[132:140] //Fee
	aa += bcd[140:142] //Trouble Mark
	aa += bcd[142:144] //Day
	aa += bcd[144:146] //A-Party Category
	aa += bcd[146:148] //Type of Call
	aa += bcd[148:150] //Bearer Service
	aa += bcd[150:154] //CUG Interlock Code
	aa += bcd[154:156] //COG OA Indicator
	aa += bcd[156:160] //UUI Messages
	aa += bcd[160:162] //Terminating Access
	aa += bcd[162:164] //Network Indicator
	aa += bcd[164:168] //Release Cause
	aa += bcd[168:170] //Supplementary Service Indicator
	return aa
}

func A9026(bcd, yy string) string {
	aa := bcd[0:2]   // Hexadecimal Identifier
	aa += bcd[2:6]   // Structure Identifier Code
	aa += bcd[6:12]  // Ticket Number
	aa += bcd[12:18] // Sequence Number
	i, _ := strconv.Atoi(bcd[18:20])
	aa += bcd[36-i : 36] // Originating Phone Number
	i, _ = strconv.Atoi(bcd[36:38])
	aa += bcd[70-i : 70]   // Terminating Phone Number
	aa += bcd[70:72]       // Charge Category
	aa += bcd[72:74]       // Nature of Call
	aa += bcd[74:76]       // CDA Indicator
	aa += bcd[76:78]       // LDC Indicator
	aa += bcd[78:80]       // Service Class of Call
	aa += yy + bcd[81:91]  // Date and Time of Charging Commencement
	aa += bcd[91:92]       //*ms
	aa += yy + bcd[93:103] // Date and Time of Call End
	aa += bcd[103:104]     //*ms
	aa += bcd[104:106]     // Cause of Call End
	aa += bcd[106:110]     // Destination
	aa += bcd[110:114]     // Outgoing Trunk Group
	aa += bcd[114:118]     // Incoming Trunk Group
	aa += bcd[118:124]     // Conversation Time
	aa += bcd[124:130]     // Chargeable Duration
	aa += bcd[130:132]     // Class of Rate
	aa += bcd[132:140]     // Fee
	aa += bcd[140:142]     // Trouble Mark
	aa += bcd[142:144]     // Day
	aa += bcd[144:146]     // A-Party Category
	aa += bcd[146:148]     // Type of Call
	aa += bcd[148:150]     // Bearer Service
	aa += bcd[150:152]     // Supplementary Service Indicator
	aa += bcd[152:154]     // Supplementary Service Action
	return aa
}

func A9050(bcd string) string {

	aa := bcd[8:10]
	aa += bcd[0:2]
	aa += bcd[2:6]
	aa += bcd[6:10]
	aa += bcd[10:14]
	aa += bcd[14:22]
	aa += bcd[22:28]
	aa += bcd[28:36]
	aa += bcd[36:42]
	aa += bcd[42:46]
	return aa
}

func A9051(bcd string) string {
	aa := bcd[0:2]
	aa += bcd[2:6]
	aa += bcd[6:10]
	aa += bcd[10:14]
	aa += bcd[14:22]
	aa += bcd[22:28]
	aa += bcd[28:36]
	aa += bcd[36:42]
	aa += bcd[42:46]
	aa += bcd[46:54]
	return aa
}

//AA ...
func AA(ad string, yr string, sw string) Redrec {
	var a Redrec
	if ad[0:6] == "AA9020" {
		a.Sw = sw
		a.Hi = ad[2:6]                   //Hexadecimal Identifier
		i, e := strconv.Atoi(ad[18:20])  //Originating Phone Number Digits
		a.Na = ad[36-i : 36]             //Originating Phone Number
		i, e = strconv.Atoi(ad[36:38])   //Terminating Phone Number Digits
		a.Nb = ad[70-i : 70]             //Terminating Phone Number
		a.Ds = yr + ad[89:99]            //Date and Time of Charging Commencement
		a.De = yr + ad[101:111]          //Date and Time of Call End
		a.Dr = ad[114:118]               //Destination
		a.Ot = ad[118:122]               //Outgoing Trunk Group
		a.It = ad[122:126]               //Incoming Trunk Group
		i, e = strconv.Atoi(ad[126:130]) //Conversation Time
		j, e := strconv.Atoi(ad[130:132])
		a.Du = strconv.Itoa((i * 60) + j)
		a.Id = a.Ds
		if a.Ds == "" {
			a.Id = a.De
		}
		Err(e)
	}
	if ad[0:6] == "AA9021" {
		a.Sw = sw
		a.Hi = ad[2:6]                  //Hexadecimal Identifier
		i, e := strconv.Atoi(ad[18:20]) //Originating Phone Number Digits
		a.Na = ad[36-i : 36]            //Originating Phone Number
		i, e = strconv.Atoi(ad[36:38])  //Terminating Phone Number Digits
		a.Nb = ad[70-i : 70]            //Terminating Phone Number
		a.Ds = yr + ad[81:91]           //Date and Time of Charging Commencement
		//a.De = yr + ad[92:104]        //Date and Time of Call End
		a.Dr = ad[106:110]               //Destination
		a.Ot = ad[110:114]               //Outgoing Trunk Group
		a.It = ad[114:118]               //Incoming Trunk Group
		i, e = strconv.Atoi(ad[124:130]) //Chargeable Duration
		a.Du = strconv.Itoa(i)
		a.Id = a.Ds
		if a.Ds == "" {
			a.Id = a.De
		}
		Err(e)
	}

	if ad[0:6] == "AA9025" {
		a.Sw = sw
		a.Hi = ad[2:6]                  //Hexadecimal Identifier
		i, e := strconv.Atoi(ad[18:20]) //Originating Phone Number Digits
		a.Na = ad[36-i : 36]            //Originating Phone Number
		i, e = strconv.Atoi(ad[36:38])  //Terminating Phone Number Digits
		a.Nb = ad[70-i : 70]            //Terminating Phone Number
		a.Ds = yr + ad[81:91]
		a.De = yr + ad[93:103]
		a.Dr = ad[106:110]               //Destination
		a.Ot = ad[110:114]               //Outgoing Trunk Group
		a.It = ad[114:118]               //Incoming Trunk Group
		i, e = strconv.Atoi(ad[118:122]) //Conversation Time
		j, e := strconv.Atoi(ad[122:124])
		a.Du = strconv.Itoa((i * 60) + j)
		a.Id = a.Ds
		if a.Ds == "" {
			a.Id = a.De
		}
		Err(e)
	}
	if ad[0:6] == "AA9026" {
		a.Sw = sw
		a.Hi = ad[2:6]                  //Hexadecimal Identifier
		i, e := strconv.Atoi(ad[18:20]) //Originating Phone Number Digits
		a.Na = ad[36-i : 36]            //Originating Phone Number
		i, e = strconv.Atoi(ad[36:38])  //Terminating Phone Number Digits
		a.Nb = ad[70-i : 70]            //Terminating Phone Number
		a.Ds = yr + ad[81:91]
		//a.De = yr + ad[93:103]
		a.Dr = ad[106:110]               //Destination
		a.Ot = ad[110:114]               //Outgoing Trunk Group
		a.It = ad[114:118]               //Incoming Trunk Group
		i, e = strconv.Atoi(ad[124:130]) //Chargeable Duration
		a.Du = strconv.Itoa(i)
		a.Id = a.Ds
		if a.Ds == "" {
			a.Id = a.De
		}
		Err(e)
	}
	if ad[0:6] == "AA0003" {
		a.Sw = sw
		a.Hi = ad[2:6]            //Hexadecimal Identifier
		a.Ot = ad[6:10]           //Outgoing Trunk Group
		a.It = ad[10:14]          //Incoming Trunk Group
		a.Ds = Dtt(yr, ad[15:25]) //Date and Time of Charging Commencement
		a.Dr = ad[28:32]          //Destination
		i, e := strconv.Atoi(ad[32:34])
		a.Nb = ad[66-i : 66]      //Terminating Phone Number
		a.De = Dtt(yr, ad[71:81]) //Date and Time of Call End
		a.Du = Diff(a.Ds, a.De)   //Duration
		a.Id = a.Ds
		if a.Ds == "" {
			a.Id = a.De
		}
		Err(e)
	}
	return a
}

//ABor - AMA begin of record
func ABor(dt string) string {
	//fmt.Println("Record Descriptor Word:", dt[0:8])
	//fmt.Println("Hexadecimal Identifier:", dt[8:10])
	//fmt.Println("Structure Identifier Code:", dt[10:14])
	//fmt.Println("Call Type:", dt[14:18])
	//fmt.Println("Recording Office Type:", dt[18:22])
	//fmt.Println("Recording Office Identification:", dt[22:30])
	//fmt.Println("Date Datalink Recording Started:", dt[30:36])
	//fmt.Println("Time Datalink Recording Started:", dt[36:44])
	//fmt.Println("Collector Program Generic Number:", dt[44:50])
	//fmt.Println("Type of Tracer:", dt[50:52])
	return "201" + dt[31:36] + dt[37:43]
}

//AEor - AMA end of record
func AEor(dt string) int {
	//fmt.Println("Record Descriptor Word:", dt[0:8])
	//fmt.Println("Hexadecimal Identifier:", dt[8:10])
	//fmt.Println("Structure Identifier Code:", dt[10:14])
	//fmt.Println("Call Type:", dt[14:18])
	//fmt.Println("Recording Office Type:", dt[18:22])
	//fmt.Println("Recording Office Identification:", dt[22:30])
	//fmt.Println("Date Datalink Recording Completed:", dt[30:36])
	//fmt.Println("Time Datalink Recording Completed:", dt[36:44])
	//fmt.Println("Collector Program Generic Number:", dt[44:50])
	//fmt.Println("Type of Tracer:", dt[50:54])
	//fmt.Println("Count of Records:", dt[54:62])
	rc, _ := strconv.Atoi(dt[54:62])
	return rc
}

//IBor - IAD begin of record
func IBor(dt string) string {
	//fmt.Println("Block ID:", dt[0:2])
	//fmt.Println("Block Sequence Counter:", dt[2:8])
	//fmt.Println("Start Date of Datalink Recording:", dt[8:14])
	//fmt.Println("Start Time of Datalink Recording:", dt[14:20])
	//fmt.Println("Filler:", dt[20:24])
	//fmt.Println("ESS Type:", dt[24:26])
	return "20" + dt[8:20]
}

//IEor - IAD end of record
func IEor(dt string) int {
	//fmt.Println("Block ID:", dt[0:2])
	//bc, _ := strconv.Atoi(dt[2:8])
	//fmt.Println("Block Sequence Counter:", bc)
	//fmt.Println("End Date of Datalink Recording:", dt[8:14])
	//fmt.Println("End Time of Datalink Recording:", dt[14:20])
	rc, _ := strconv.Atoi(dt[20:28])
	//fmt.Println("Count of Records on Tape:", rc)
	return rc
}

func Diff(ds, de string) string {
	// setup a start and end time
	if ds == "" {
		return ""
	}
	if de == "" {
		return ""
	}
	createdAt, _ := time.Parse("20060102150405", ds)
	expiresAt, _ := time.Parse("20060102150405", de)
	// get the diff
	diff := expiresAt.Sub(createdAt).Nanoseconds() / 1000000000
	diff = int64(diff)
	dif := strconv.FormatInt(diff, 10)
	return dif
}

func Dtt(yr, ds string) string {
	// setup a start and end time
	d := yr + ds
	dss, _ := strconv.Atoi(ds)

	if dss == 0 {
		d = ""
	}
	return d

}

//Err - error
func Err(e error) {
	if e != nil {
		panic(e)
	}
}
