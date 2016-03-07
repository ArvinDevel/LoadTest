/**
 * Created by arvin on 16-3-2.
 */
import scala.runtime.ScalaRunTime

/**
 * A sample code to implement a customized case class manually (for Scala 2.9.2 final).
 *
 * Id has two properties: name and displayName. 'displayName' has default value that
 * is same value as 'name'.
 */
class CustomCaseClass private ( // make primary constructor not to be accessible. (standard case class can do)
                             val nfVersion : Int,
                                val flowSetCount: Int,
                                val sysUptime: Int,
                                val epochTime: Int,
                                val nanoSeconds: Int,
                                val flowsSeen: Int,
                                val engineType: Int,
                                val engineId: Int,
                                val samplingInfo: Int,

                                val srcAddr: String,
                                val dstAddr: String,
                                val nexthop: String,
                                val input: Long,
                                val output: Long,
                                val dPkts: Long,
                                val dOctets: Long,
                                val first: Long,
                                val last: Long,
                                val srcport: Int,
                                val destPort: Int,
                                val tcp_flags: Int,
                                val prot: Int,
                                val tos: Int,
                                val src_as: Long,
                                val dst_as: Long,
                                val src_mask: Int,
                                val dst_mask: Int) extends Product with Serializable { // inherits from Product and Serializable.

  // implement copy method manually.
  def copy(
            nfVersion : Int = this.nfVersion,
            flowSetCount: Int = this.flowSetCount,
            sysUptime: Int = this.sysUptime,
            epochTime: Int = this.epochTime,
            nanoSeconds: Int = this.nanoSeconds,
            flowsSeen: Int = this.flowsSeen,
            engineType: Int = this.engineType,
            engineId: Int = this.engineId,
            samplingInfo: Int = this.samplingInfo,

            srcAddr: String = this.srcAddr,
            dstAddr: String = this.dstAddr,
            nexthop: String = this.nexthop,
            input: Long = this.input,
            output: Long = this.output,
            dPkts: Long = this.dPkts,
            dOctets: Long = this.dOctets,
            first: Long = this.first,
            last: Long = this.last,
            srcport: Int = this.srcport,
            destPort: Int = this.destPort,
            tcp_flags: Int = this.tcp_flags,
            prot: Int = this.prot,
            tos: Int = this.tos,
            src_as: Long = this.src_as,
            dst_as: Long = this.dst_as,
            src_mask: Int = this.src_mask,
            dst_mask: Int = this.dst_mask) =
    CustomCaseClass.apply( nfVersion ,flowSetCount,sysUptime,epochTime, nanoSeconds,flowsSeen, engineType, engineId, samplingInfo,

      srcAddr,
      dstAddr,
      nexthop,
      input,
      output,
      dPkts,
      dOctets,
      first,
      last,
      srcport,
      destPort,
      tcp_flags,
      prot,
      tos,
      src_as,
      dst_as,
      src_mask,
      dst_mask)

  // Product (used by Scala runtime)

  override def productPrefix = classOf[CustomCaseClass].getSimpleName

  def productArity = 27

  def productElement(n: Int): Any = n match {
    case 0 => this.nfVersion
    case 1 => this.flowSetCount
    case 2 => this.sysUptime
    case 3 => this.epochTime
    case 4 => this.nanoSeconds
    case 5 => this.flowsSeen
    case 6 => this.engineType
    case 7 => this.engineId
    case 8 => this.samplingInfo
    case 9 => this.srcAddr
    case 10 => this.dstAddr
    case 11 => this.nexthop
    case 12 => this.input
    case 13 => this.output
    case 14 => this.dPkts
    case 15 => this.dOctets
    case 16 => this.first
    case 17 => this.last
    case 18 => this.srcport
    case 19 => this.destPort
    case 20 => this.tcp_flags
    case 21 => this.prot
    case 22 => this.tos
    case 23 => this.src_as
    case 24 => this.dst_as
    case 25 => this.src_mask
    case 26 => this.dst_mask
    case _ => throw new IndexOutOfBoundsException(n.toString)
  }

  def canEqual(that: Any) = that.isInstanceOf[CustomCaseClass]

  // NOTE: Scaladoc of ScalaRunTime says that "All these methods should be considered
  // outside the API and subject to change or removal without notice."
  override def equals(that: Any) = ScalaRunTime._equals(this, that)

  // Object (AnyRef)

  override def hashCode() = ScalaRunTime._hashCode(this)

  override def toString = ScalaRunTime._toString(this)

}

object CustomCaseClass {

  // exclusive factory method
  def apply(
             nfVersion : Int,
             flowSetCount: Int,
             sysUptime: Int,
             epochTime: Int,
             nanoSeconds: Int,
             flowsSeen: Int,
             engineType: Int,
             engineId: Int,
             samplingInfo: Int,

             srcAddr: String,
             dstAddr: String,
             nexthop: String,
             input: Long,
             output: Long,
             dPkts: Long,
             dOctets: Long,
             first: Long,
             last: Long,
             srcport: Int,
             destPort: Int,
             tcp_flags: Int,
             prot: Int,
             tos: Int,
             src_as: Long,
             dst_as: Long,
             src_mask: Int,
             dst_mask: Int) =
   new CustomCaseClass(nfVersion ,flowSetCount,sysUptime,epochTime, nanoSeconds,flowsSeen, engineType, engineId, samplingInfo,
     srcAddr, dstAddr, nexthop, input, output, dPkts, dOctets, first, last, srcport, destPort, tcp_flags, prot, tos, src_as, dst_as, src_mask, dst_mask)

//  Error:(181, 56) object Tuple27 is not a member of package scala
//  def unapply(customCaseClass: CustomCaseClass) = Some((customCaseClass.nfVersion, customCaseClass.flowSetCount,customCaseClass.sysUptime,


  // for pattern matching
//  def unapply(customCaseClass: CustomCaseClass) = Some((customCaseClass.nfVersion, customCaseClass.flowSetCount,customCaseClass.sysUptime,
//    customCaseClass.epochTime, customCaseClass.nanoSeconds, customCaseClass.flowsSeen, customCaseClass.engineType, customCaseClass.engineId, customCaseClass.samplingInfo,
//
//  customCaseClass.srcAddr,
//  customCaseClass.dstAddr,
//  customCaseClass.nexthop,
//  customCaseClass.input,
//  customCaseClass.output,
//  customCaseClass.dPkts,
//  customCaseClass.dOctets,
//  customCaseClass.first,
//  customCaseClass.last,
//  customCaseClass.srcport,
//  customCaseClass.destPort,
//  customCaseClass.tcp_flags,
//  customCaseClass.prot,
//  customCaseClass.tos,
//  customCaseClass.src_as,
//  customCaseClass.dst_as,
//  customCaseClass.src_mask,
//  customCaseClass.dst_mask))

}

